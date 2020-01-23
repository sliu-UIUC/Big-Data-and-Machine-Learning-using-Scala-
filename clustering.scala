package sparkml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import swiftvis2.plotting.styles.ScatterStyle

object clustering extends App {
	val spark = SparkSession.builder.master("local[*]").appName("clustering").getOrCreate()
	import spark.implicits._
	spark.sparkContext.setLogLevel("WARN")
	
//*******Question 1********
	val aggLvlData = spark.read.format("csv").option("header", "true").load("../../../../data/BigData/bls/qcew/agglevel_titles.csv").toDF("aggLvlCode","aggLvlTitle" )
	val mainData   = spark.read.format("csv").option("header", "true").load("../../../../data/BigData/bls/qcew/2016.q1-q4.singlefile.csv")
	val aggLvlInfo = mainData.join(aggLvlData, 'aggLvlCode==='agglvl_code).filter('aggLvlTitle.contains("County,")).groupBy('agglvl_code).agg(count('agglvl_code))
	aggLvlInfo.show()
/*
+-----------+------------------+                                                
|agglvl_code|count(agglvl_code)|
+-----------+------------------+
|         73|            277932|
|         71|             51964|
|         70|             13084|
|         75|           1100372|
|         78|           3984876|
|         77|           3396052|
|         72|             76460|
|         74|            406868|
|         76|           2162440|
+-----------+------------------+
*/

//*******Question 2********
	val areaInfo   = spark.read.format("csv").option("header", "true").load("../../../../data/BigData/bls/qcew/area_titles.csv").toDF("areaFips", "areaTitle")
	val bexarCount = mainData.join(areaInfo, 'area_fips==='areaFips).filter('areaTitle.contains("Bexar County")).agg(count('areaFips))
	bexarCount.show() //9244

//*******Question 3********
	val industryInfo    = spark.read.format("csv").option("header", "true").load("../../../../data/BigData/bls/qcew/industry_titles.csv").toDF("industryCode", "industryTitle")
	val popularIndustry = mainData.join(industryInfo, 'industryCode==='industry_code).groupBy('industry_code).agg(count('industry_code)).toDF("industry_code", "count").orderBy('count.desc)
	popularIndustry.show(3)
/*
+-------------+-----+                                                           
|industry_code|count|
+-------------+-----+
|           10|76952|
|          102|54360|
|         1025|40588|
+-------------+-----+
*/
//*******Question 4********
	val wealthyInd = mainData.filter('agglvl_code===78).select('industry_code,'qtr, 'total_qtrly_wages).groupBy('industry_code).agg(sum('total_qtrly_wages) as "totalWages").join(industryInfo, 'industry_code==='industryCode).select('industry_code, 'industryTitle, 'totalWages).orderBy('totalWages.desc)
	wealthyInd.show(3,false)
/*
+-------------+---------------------------------------------------+----------------------+
|industry_code|industryTitle                                      |sum(total_qtrly_wages)|
+-------------+---------------------------------------------------+----------------------+
|611110       |NAICS 611110 Elementary and secondary schools      |2.4432185309E11       |
|551114       |NAICS 551114 Managing offices                      |2.1938760563E11       |
|622110       |NAICS 622110 General medical and surgical hospitals|2.05806016743E11      |
+-------------+---------------------------------------------------+----------------------+
*/

//*******Question 5********
	
	val electionData = spark.read.format("csv").option("header","true").
	    load("../../../../data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").
	    filter('county_name!=="Alaska")
	    
	val k2Converter       = udf((row:Double) => if(row>0.5) 1 else 0)
  val k2ClusterCols     = Array("month3_emplvl", "taxable_qtrly_wages", "avg_wkly_wage", "oty_month3_emplvl_chg")
  val k2DataWithType    = k2ClusterCols.foldLeft(mainData)((df, col)=>df.withColumn(col, df(col).cast(IntegerType).as(col)))
  .drop('disclosure_code).drop('oty_disclosure_code).drop('lq_disclosure_code).na.drop()
  val k2Assembler       = new VectorAssembler().setInputCols(k2ClusterCols).setOutputCol("features")
  val k2DataWithFeature = k2Assembler.transform(k2DataWithType)
  val normalizer        = new Normalizer().setInputCol("features").setOutputCol("normalizedFeatures")
  val normalizedData    = normalizer.transform(k2DataWithFeature)
  normalizedData.show(false)
  
  val accuracy    = udf((lhs:Integer, rhs:Integer)=>if(lhs==rhs) 1 else 0)
  
  val kmeans2 = new KMeans().setK(2).setFeaturesCol("normalizedFeatures")
  val model2 = kmeans2.fit(normalizedData)
  //println("Debug Level 1")
  val prediction2 = model2.transform(normalizedData)
  val goodPred2   = prediction2.withColumn("int_area_fips", col("area_fips").cast(IntegerType))
  val goodElec2   = electionData.withColumn("int_combined_fips", col("combined_fips").cast(IntegerType)).
                    withColumn("norm_cluster", k2Converter(col("per_dem")))
  val joinedData2 = goodPred2.join(goodElec2, 'int_combined_fips === 'int_area_fips)
  val resultCol   = joinedData2.select('prediction, 'norm_cluster).withColumn("accuracy", 
                    accuracy('prediction, 'norm_cluster))
  val resultAccu  = resultCol.filter('accuracy===1).count() / resultCol.count().toDouble
  println ("Result accuracy with 2 clusters is: "+resultAccu) //33.43%
	
 
  val kmeans3 = new KMeans().setK(3).setFeaturesCol("normalizedFeatures")
  val model3 = kmeans3.fit(normalizedData)
  val k3Converter = udf((row:Double) => if(row>0.6) 2 else if(row<0.4) 0 else 1)
  
  val prediction3 = model3.transform(normalizedData)
  val goodPred3   = prediction3.withColumn("int_area_fips", col("area_fips").cast(IntegerType))
  val goodElec3   = electionData.withColumn("int_combined_fips", col("combined_fips").cast(IntegerType)).
                    withColumn("norm_cluster", k3Converter(col("per_dem")))
  val joinedData3 = goodPred3.join(goodElec3, 'int_combined_fips === 'int_area_fips)
  val resultCol3  = joinedData3.select('prediction, 'norm_cluster).withColumn("accuracy", 
                    accuracy('prediction, 'norm_cluster))
  val resultAccu3 = resultCol3.filter('accuracy===1).count() / resultCol3.count().toDouble
  println ("Result accuracy with 3 clusters is: "+resultAccu3) //31.92%
  
//*******Question 6********
  val elecData    = joinedData2.filter('state_abbr !== "AK").groupBy('county_name, 'state_abbr, 'prediction.
      alias("model_prediction")).agg(avg('votes_dem).alias("votes_dem"),avg('votes_gop).alias("votes_gop"))
	val zipCodeData = spark.read.option("header", true).csv("../../../../data/BigData/bls/zip_codes_states.csv")
	val aggZipData  = zipCodeData.filter('state !== "AK").groupBy('county,'state).agg(avg('latitude).alias("latitude"),avg('longitude).alias("longitude"))
	val elecZipData = elecData.join(aggZipData,(elecData.col("county_name").contains($"county")))
	val locCols     = Array("latitude", "longitude")
	
	val plotDataWithTP   = locCols.foldLeft(elecZipData)((df, colName) => df.withColumn(colName, df(colName).cast(DoubleType).as(colName))).na.drop()
	val plotVecAssemp    = new VectorAssembler().setInputCols(locCols).setOutputCol("location")
	val vectPlotData     = plotVecAssemp.transform(plotDataWithTP) 
	val normPlotData     = new Normalizer().setInputCol("location").setOutputCol("normLocation").transform(vectPlotData)
	val kMeans           = new KMeans().setK(2).setFeaturesCol("location")
	val plotModel        = kMeans.fit(vectPlotData)
	  
	val plotDTWithClstr  = plotModel.transform(vectPlotData)
	val predict2 = plotDTWithClstr.select('model_prediction).as[Double].collect()
	
	val colorGrad2 = ColorGradient(0.0 -> BlueARGB, 1.0 -> RedARGB)
	val colorGrad3 = ColorGradient(0.0 -> BlueARGB, 0.4-> BlackARGB, 0.6 -> RedARGB)
	val plot = Plot.scatterPlot(plotDTWithClstr.select('longitude).as[Double].collect(), plotDTWithClstr.select('latitude).as[Double].collect(),
	          "Election Result with 2 clusters", "Longitude", "Latitude", 2, predict2.map(colorGrad2))
  SwingRenderer(plot, 1000, 1000, true)
  
  
	spark.stop()


}
