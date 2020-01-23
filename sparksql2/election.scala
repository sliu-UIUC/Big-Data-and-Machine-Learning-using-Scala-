package sparksql2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import org.apache.spark.ml.linalg
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.stat.Correlation

import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.styles.HistogramStyle.DataAndColor


object election extends App {
	val spark = SparkSession.builder().master("local[*]").appName("Election").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")
	
	val electionData = spark.read.format("csv").option("header","true").
	    load("../../../../data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").
	    filter('county_name!=="Alaska").select('state_abbr, 'county_name,
	    'votes_dem.cast("Double"), 'votes_gop.cast("Double"), 
	    'total_votes.cast("Double"), 'per_dem.cast("Double"), 
	    'per_gop.cast("Double"), 'diff,
	    'per_point_diff//, 'combined_fips.cast("Integer")
	    )
	
	electionData.show()
	val numCounties = electionData.count()
	// idx, votes_dem, votes_gop, total_votes, per_dem, per_gop,
	// diffper_point_diff, state_abbr,county_name,combined_fips
//Q1:
	//majority > 10%
	val majGOP = electionData.filter('per_gop>'per_dem).count().toDouble / numCounties
	println(majGOP)
	
//Q2: 
	//using per_point_diff instead of per_gop/ per_dem >=1.1
	val bigwinGOP = electionData.filter('per_gop - 'per_dem >= 0.1).count().toDouble / numCounties //0.7869537275064268
  val bigwinDEM = electionData.filter('per_dem - 'per_gop >= 0.1).count().toDouble / numCounties //0.107969151670951161

	println("Fraction of republican won by 10%: "+bigwinGOP)
	println("Fraction of democratic won by 10%: "+bigwinDEM)

//Q3: 
  val totVotesData    = electionData.select('total_votes).rdd.map(_(0).asInstanceOf[Double]).collect()
  val percentDiffData = electionData.select('per_dem - 'per_gop).rdd.map(_(0).asInstanceOf[Double]).collect()
  val electResultPlot = Plot.scatterPlot(totVotesData, percentDiffData, 
       "Election result by Total votes against Democra/Republic difference",
       "Total number of votes", "% Democratic votes minus % Republican votes",
       1.5, BlackARGB)
  SwingRenderer(electResultPlot, 1000, 800, true)

//Q4: 
  val percentDemData = electionData.select('county_name, 'per_dem)
  val geoData = spark.read.format("csv").option("header","true").
      load("../../../../data/BigData/bls/zip_codes_states.csv").
      filter(('state!=="PR") && ('state !=="AK") && ('state !=="HI") ).
      select('latitude.cast("Double"), 'longitude.cast("Double"), 'county).distinct()
  val countyWithGeo = geoData.join(percentDemData, 'county_name.contains('county))
      .select('county, 'latitude, 'longitude, 'per_dem).distinct()
  val goodData      = countyWithGeo.filter('latitude.isNotNull && 'longitude.isNotNull && 'per_dem.isNotNull)
  val valueData     = goodData.select('per_dem).rdd.map(_(0).asInstanceOf[Double]).collect()
  val latitudeData  = goodData.select('latitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  val longitudeData = goodData.select('longitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  val colorGrad     = ColorGradient(0.40 -> RedARGB, 0.60 -> BlueARGB)
  val plotWithGeo   = Plot.scatterPlot(longitudeData, latitudeData, "Political affiliation for mainland U.S counties",
        "Longitude", "Latitude", 3.6, colorGrad(valueData))
  SwingRenderer(plotWithGeo, 800, 800, true)
  countyWithGeo.show()

//Q5:
  val stateSchema = StructType(Array(
		                StructField("series_id", StringType), 
                    StructField("year", IntegerType),
                    StructField("period", StringType),
                    StructField("value", DoubleType),
                    StructField("footnote_code", StringType)
	))
	val combinedData = spark.read.schema(stateSchema).
		option("header", "true").option("delimiter", "\t").
		csv("../../../../data/BigData/bls/la/la.data.concatenatedStateFiles")
	//combinedData.describe().show()
	
  val areaSchema = StructType(Array( 
                   StructField("area_type_code", StringType),
                   StructField("area_code", StringType),
                   StructField("area_text", StringType),
                   StructField("display_level", IntegerType),
                   StructField("selectable", StringType),
                   StructField("sort_sequence", StringType)
	))
	
	val laAreaData = spark.read.schema(areaSchema).
		option("header", "true").option("delimiter", "\t").
		csv("../../../../data/BigData/bls/la/la.area")
  
	val metropolitanAreas = laAreaData.filter('area_type_code === "B")
	val micropolitanAreas = laAreaData.filter('area_type_code === "D")
	val countyEqAreas     = laAreaData.filter('area_type_code === "F")
//	metropolitanAreas.show(false)
//	micropolitanAreas.show(false)
//	countyEqAreas.show(false)
  val data1990June = combinedData.filter('year===1990 && 'period==="M06" 
                     && trim('series_id).endsWith("03")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15)))
  val data1991May  = combinedData.filter('year===1991 && 'period==="M03" 
                     && trim('series_id).endsWith("03")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15)))
  val data2001Feb  = combinedData.filter('year===2001 && 'period==="M02" 
                     && trim('series_id).endsWith("03")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15)))
  val data2001Nov  = combinedData.filter('year===2001 && 'period==="M11" 
                     && trim('series_id).endsWith("03")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15)))
  val data2007Nov  = combinedData.filter('year===2007 && 'period==="M11" 
                     && trim('series_id).endsWith("03")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15)))
  val data2009June = combinedData.filter('year===2009 && 'period==="M06" && 
                     trim('series_id).endsWith("03")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15)))
  //data1990June.show(false)
  
  //excluding Puerto Rico and DC because they are not in the 50 states
  //Metropolitan Areas
  val statesMetro1990June = data1990June.join(metropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
  //val stateLst = statesMetro1990June.select('state).rdd.map(_(0).asInstanceOf[String]).collect()
  val unemplyMetro1990June = statesMetro1990June.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
  val statesMetro1991May = data1991May.join(metropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMetro1991May = statesMetro1991May.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()   

  val statesMetro2001Feb = data2001Feb.join(metropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMetro2001Feb = statesMetro2001Feb.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
  val statesMetro2001Nov = data2001Nov.join(metropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMetro2001Nov = statesMetro2001Nov.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
   
  val statesMetro2007Nov = data2007Nov.join(metropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMetro2007Nov = statesMetro2007Nov.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
                             
  val statesMetro2009June = data2009June.join(metropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMetro2009June = statesMetro2009June.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
	val metroLst = Array(statesMetro1990June, statesMetro1991May, statesMetro2001Feb, 
	                     statesMetro2001Nov , statesMetro2007Nov, statesMetro2009June )
//	println("Show metropolitan list: ")
//	for(i <- 0 to metroLst.size-1) {
//	  metroLst(i).show()
//	}  
	                     
	//micropolitan areas
	val statesMicro1990June = data1990June.join(micropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
  val unemplyMicro1990June = statesMicro1990June.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
  val statesMicro1991May = data1991May.join(micropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMicro1991May = statesMicro1991May.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()   

  val statesMicro2001Feb = data2001Feb.join(micropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMicro2001Feb = statesMicro2001Feb.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
  val statesMicro2001Nov = data2001Nov.join(micropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMicro2001Nov = statesMicro2001Nov.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
   
  val statesMicro2007Nov = data2007Nov.join(micropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMicro2007Nov = statesMicro2007Nov.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
                             
  val statesMicro2009June = data2009June.join(micropolitanAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyMicro2009June = statesMicro2009June.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
	val microLst = Array(statesMicro1990June, statesMicro1991May, statesMicro2001Feb, 
	                     statesMicro2001Nov , statesMicro2007Nov, statesMicro2009June )
	
//  println("Show micropolitan list: ")
//	for(i <- 0 to microLst.size-1) {
//	  microLst(i).show()
//	}                       
	//counties and equivalent                   
  val statesCounty1990June = data1990June.join(countyEqAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
  //val stateLst = statesMetro1990June.select('state).rdd.map(_(0).asInstanceOf[String]).collect()
  val unemplyCounty1990June = statesCounty1990June.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
  val statesCounty1991May = data1991May.join(countyEqAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyCounty1991May = statesCounty1991May.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()   

  val statesCounty2001Feb = data2001Feb.join(countyEqAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyCounty2001Feb = statesCounty2001Feb.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
  val statesCounty2001Nov = data2001Nov.join(countyEqAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyCounty2001Nov = statesCounty2001Nov.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
   
  val statesCounty2007Nov = data2007Nov.join(countyEqAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyCounty2007Nov = statesCounty2007Nov.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
                             
  val statesCounty2009June = data2009June.join(countyEqAreas, 'area_code === 'series_id).
                        select('area_text, 'value).withColumn("state", 
                        split(col("area_text"), ", ").getItem(1).substr(0, 2)).
                        filter(('state !=="PR") && ('state!=="DC")).
                        select('state, 'value).orderBy('state).groupBy('state).avg("value")
                        
  val unemplyCounty2009June = statesCounty2009June.select(col("avg(value)")).rdd.
                             map(_(0).asInstanceOf[Double]).collect()
  
	val countyLst = Array(statesCounty1990June, statesCounty1991May, statesCounty2001Feb, 
	                     statesCounty2001Nov , statesCounty2007Nov, statesCounty2009June )
//  println("Show counties or equivalent list: ")
//	for(i <- 0 to metroLst.size-1) {
//	  countyLst(i).show()
//	}  
	val bins = (0.0 to 50.0 by 1.0).toArray 
	//Plot.histogramGrid(bins, valsAndColors, centerOnBins, sharedYAxis, title, xLabel, yLabel, binsOnX)
	val histPlotGrid = Plot.histogramGrid(bins, Seq(
	      Seq(DataAndColor(unemplyMetro1990June, GreenARGB),DataAndColor(unemplyMetro1991May, RedARGB), 
	          DataAndColor(unemplyMetro2001Feb, GreenARGB),DataAndColor(unemplyMetro2001Nov, RedARGB),
	          DataAndColor(unemplyMetro2007Nov, GreenARGB),DataAndColor(unemplyMetro2009June, RedARGB)
	      ), 
	      Seq(DataAndColor(unemplyMicro1990June, GreenARGB),DataAndColor(unemplyMicro1991May, RedARGB), 
	          DataAndColor(unemplyMicro2001Feb, GreenARGB),DataAndColor(unemplyMicro2001Nov, RedARGB),
	          DataAndColor(unemplyMicro2007Nov, GreenARGB),DataAndColor(unemplyMicro2009June, RedARGB)
	      ),
	      Seq(DataAndColor(unemplyCounty1990June, GreenARGB),DataAndColor(unemplyCounty1991May, RedARGB), 
	          DataAndColor(unemplyCounty2001Feb, GreenARGB),DataAndColor(unemplyCounty2001Nov, RedARGB),
	          DataAndColor(unemplyCounty2007Nov, GreenARGB),DataAndColor(unemplyCounty2009June, RedARGB)
	      )
	      ),false, false, "Histogram Grid before and after each recession for Metropolitan, Micropolitan and county areas",
	      "State Index", "Unemployment Rate")
	SwingRenderer(histPlotGrid, 1200, 1200)
	
	
//Q6: 
	//a: 
	println("")
	println("Question 6 test: ")
	//	val countyEqAreas     = laAreaData.filter('area_type_code === "F")
	  val data2016Nov = combinedData.filter('year===2016 && 'period==="M11" 
                     && trim('series_id).endsWith("03")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15)))
    val countiesUnemplymt2016Nov = data2016Nov.join(countyEqAreas, 'area_code === 'series_id).select('series_id,'area_text, 'value)
    val countiesCorrData2016Nov = countiesUnemplymt2016Nov.join(electionData, 'area_text.contains('county_name) 
                                  && 'area_text.contains('state_abbr)).select('series_id, 'area_text, 'value, 'per_dem, 'per_gop, 'total_votes)
//    countiesUnemplymt2016Nov.show(false)
//    println(countiesUnemplymt2016Nov.count())
//    countiesCorrData2016Nov.show(false)
//    println(countiesCorrData2016Nov.count())
    val corrUnemplyAndDem = countiesCorrData2016Nov.stat.corr("value", "per_dem") //0.16672925713740164
    println("Correlation Coefficient between unemployment and percentage of democratic votes is: "+corrUnemplyAndDem)
    //b: 
    val laborForce2016Nov = combinedData.filter('year===2016 && 'period==="M11" 
                     && trim('series_id).endsWith("06")).
                     withColumn("series_id", regexp_replace(col("series_id"), 
                     col("series_id"), col("series_id").substr(4,15))).join(
                     countyEqAreas, 'area_code === 'series_id).select('area_code, 'value).
                     toDF("area_code", "labor_force")
//    laborForce2016Nov.show(false) 
//    println(laborForce2016Nov.count())
    val tripleData = countiesCorrData2016Nov.join(laborForce2016Nov,'series_id==='area_code)
    .select('series_id,'value, 'labor_force, 'per_dem,'per_gop, 'total_votes)
    tripleData.show(false)
    val unemplyData = tripleData.select('value).rdd.map(_(0).asInstanceOf[Double]).collect()
    val laborForceData = tripleData.select('labor_force).rdd.map(_(0).asInstanceOf[Double]).collect()
//    println("Maximum county labor force: "+laborForceData.max)
//    println("Minimum county labor force: "+laborForceData.min)
    val perDemData = tripleData.select('per_dem).rdd.map(_(0).asInstanceOf[Double]).collect()
    val colorGrad2 = ColorGradient(10000.0 -> GreenARGB, 50000.0 -> BlueARGB, 
                                   100000.0 -> YellowARGB, 1000000.0 -> RedARGB)
    val corrPlot = Plot.scatterPlot(unemplyData, perDemData, "Correlation plot between unemployment and percent democratic vote in 2016",
           "Unemployment Rate", "Percentage Democratic Vote", 5, colorGrad2(laborForceData))
    SwingRenderer(corrPlot, 1000, 1000, true)

//Q7: 
   val transformedData    = tripleData.withColumn("turnout", 'total_votes/'labor_force).select('value, 'per_dem, 'per_gop, 'turnout)
   val turnoutUnemplyCorr = transformedData.stat.corr("turnout", "value")
   val turnoutPerDemCorr  = transformedData.stat.corr("turnout", "per_dem")
   val turnoutPerGopCorr  = transformedData.stat.corr("turnout", "per_gop")
   println("Correlation coefficient bettween voter turnout and unemployment: "+turnoutUnemplyCorr) //0.1773494983384578
   println("Correlation coefficient bettween voter turnout and percentage democratic vote: "+turnoutPerDemCorr) //-0.034695985794154956
   println("Correlation coefficient bettween voter turnout and percentage republic vote: "+turnoutPerGopCorr) //0.05403469166804504

}
