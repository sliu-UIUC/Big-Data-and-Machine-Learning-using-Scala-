package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import swiftvis2.plotting.styles.ScatterStyle

object unemployment extends App {
	val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
	import spark.implicits._
	spark.sparkContext.setLogLevel("WARN")

	val stateSchema = StructType(Array(
		StructField("series_id", StringType), 
                StructField("year", IntegerType),
                StructField("period", StringType),
                StructField("value", DoubleType),
                StructField("footnote_code", StringType)
	))
	
	val newMexicoData = spark.read.schema(stateSchema).
		option("header", "true").option("delimiter", "\t").
		csv("../../../../data/BigData/bls/la/la.data.38.NewMexico")
//Q1:
	newMexicoData.describe().show() //5 columns(series id, year, period, value and footnote code; 99496 data entries

//Q2:
	newMexicoData.createOrReplaceTempView("newMexicoSQL")
	val maxUnemplySQLWay = spark.sql("""SELECT MAX(*) FROM (SELECT value FROM newMexicoSQL WHERE TRIM(series_id) LIKE '%04') """).show()
	val maxUnemplySpark  =  newMexicoData.filter(trim('series_id).endsWith("04")).agg(max('value)).show() //86751.0

//Q3:
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

	val numLargeCTsSpark =  laAreaData.filter('area_type_code==="G" && 'area_text.endsWith("NM")).agg(count('area_text)).show() //10
	laAreaData.createOrReplaceTempView("largeCitySQL")
	val numLargeCTsSQL   = spark.sql(""" SELECT COUNT(*) FROM largeCitySQL WHERE area_type_code ='G' AND area_text LIKE '%NM'""").show()
	
//Q4:
//a:
	 val unemplyByMonthSpark = newMexicoData.filter(trim('series_id).endsWith("03") && 'year===2017).select('period,'value).orderBy("period").groupBy("period").avg().show()
	val unemplyByMonthSQL = spark.sql("""SELECT period, AVG(value) FROM newMexicoSQL WHERE TRIM(series_id) LIKE '%03' AND year = '2017' GROUP BY period ORDER BY period""").show()
//6.449230769230769
/*
+------+------------------+
|period|        avg(value)|
+------+------------------+
|   M01|7.1909090909090905|
|   M02| 6.957575757575758|
|   M03| 6.612121212121213|
|   M04| 6.169696969696969|
|   M05| 6.095454545454547|
|   M06| 6.971212121212122|
|   M07| 6.872727272727273|
|   M08| 6.427272727272728|
|   M09| 6.133333333333334|
|   M10| 5.965151515151516|
|   M11| 6.042424242424242|
|   M12|  5.91969696969697|
|   M13| 6.449230769230769|
+------+------------------+*/


//b:
	val countiesNM = laAreaData.filter('area_type_code==="F" && 'area_text.endsWith("NM")).select('area_code)//all the counties in New Mexico
	val countiesData = newMexicoData.filter(trim('series_id).endsWith("03") && 'year===2017) //original newMexico data
	val updatedCountiesData = countiesData.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(4,15))) //create a new dataframe series with updated series id to match the area code in countiesNM
	val countiesData2017 = updatedCountiesData.join(countiesNM, 'series_id === 'area_code).filter('period ==="M13") //only looking for average yearly data
	val avgUnemplymtSpark = countiesData2017.agg(avg('value)).show()
/*
+-----------------+
|       avg(value)|
+-----------------+
|6.557575757575757|
+-----------------+*/

//Q5: 
	val laborForceData = newMexicoData.filter(trim('series_id).endsWith("06") && 'year===2017)
	val updatedLaborForce = laborForceData.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(4,15)))
	val countyMonthlyLaborForce = updatedLaborForce.join(countiesNM, 'series_id === 'area_code).select('period, 'value).orderBy("period").groupBy("period").avg().toDF("month", "laborForce")
	val unemployed = newMexicoData.filter(trim('series_id).endsWith("04") && 'year===2017)
	val updatedUnemployed = unemployed.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(4,15)))
	val countyMonthlyUnemplymt  = updatedUnemployed.join(countiesNM, 'series_id === 'area_code).select('period, 'value).orderBy("period").groupBy("period").avg().toDF("month","unemployment" )
	val combinedData = countyMonthlyUnemplymt.join(countyMonthlyLaborForce, countyMonthlyUnemplymt.col("month") === countyMonthlyLaborForce.col("month")).select(countyMonthlyUnemplymt.col("month"), 'unemployment, 'laborforce).orderBy('month)
	val weightedAverage = combinedData.select('unemployment / 'laborforce).show()
// 0.06152186167397469

//Q6: 
	val texasData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.51.Texas")
	val laborForceTX = texasData.filter(trim('series_id).endsWith("06") && 'value >=10000).select('series_id).toDF("id")
	val uL = laborForceTX.withColumn("id", regexp_replace(col("id"),col("id"), col("id").substr(0,18)))
	val unemployRateTX = texasData.filter(trim('series_id).endsWith("03")).select('series_id, 'value, 'year, 'period)
	val uU = unemployRateTX.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(0,18)))
	val unemployPop = uL.join(uU, 'id === 'series_id).select('id, 'year, 'period, 'value).distinct.sort('value.desc).show()
/*
|LAUCN4842700000000|1990|   M02| 54.1| Starr County, TX
|LAUMC4840100000000|1990|   M02| 54.1| Rio Grande City, TX Micropolitan Statistical Area 
*/

//Q7 
	val arkansasData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.10.Arkansas")
	val californiaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.11.California")
        val coloradoData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.12.Colorado")
        val connecticutData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.13.Connecticut")
        val delawareData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.14.Delaware")
        val dcData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.15.DC")
        val floridaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.16.Florida")
        val georgiaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.17.Georgia")
	val hawaiiData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.18.Hawaii")
	val idahoData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.19.Idaho")
	val illinoisData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.20.Illinois")
	val indianaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.21.Indiana")
	val iowaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.22.Iowa")
	val kansasData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.23.Kansas")
	val kentuckyData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.24.Kentucky")
	val louisianaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.25.Louisiana")
	val maineData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.26.Maine")
	val marylandData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.27.Maryland")
	val massachusettsData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.28.Massachusetts")
	val michiganData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.29.Michigan")
	val minnesotaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.30.Minnesota")
	val mississippiData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.31.Mississippi")
	val missouriData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.32.Missouri")
	val montanaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.33.Montana")
	val nebraskaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.34.Nebraska")
	val nevadaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.35.Nevada")
	val newHampshireData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.36.NewHampshire")
	val newJerseyData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.37.NewJersey")
	val newYorkData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.39.NewYork")
	val northCarolinaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.40.NorthCarolina")
	val northDakotaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.41.NorthDakota")
	val ohioData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.42.Ohio")
	val oklahomaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.43.Oklahoma")
	val oregonData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.44.Oregon")
	val pennsylvaniaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.45.Pennsylvania")
	val puertoRicoData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.46.PuertoRico")
	val rhodeIslandData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.47.RhodeIsland")
	val southCarolinaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.48.SouthCarolina")
	val southDakotaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.49.SouthDakota")
	val tennesseeData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.50.Tennessee")
	val utahData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.52.Utah")
	val vermontData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.53.Vermont")
	val virginiaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.54.Virginia")
	val washingtonData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.56.Washington")
	val westVirginiaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.57.WestVirginia")
	val wisconsinData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.58.Wisconsin")
	val wyomingData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.59.Wyoming")
	val alabamaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.7.Alabama")
	val alaskaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.8.Alaska")
	val arizonaData = spark.read.schema(stateSchema).
                option("header", "true").option("delimiter", "\t").
                csv("../../../../data/BigData/bls/la/la.data.9.Arizona")

//use the same process as 6
//Arizona Data: |LAUCT0463470000000|2013|   M07| 58.7| San Luis city, AZ
	
//Q8: 
	val seriesSchema = StructType(Array(
                StructField("series_id", StringType),
                StructField("area_type_code", StringType),
                StructField("area_code", StringType),
                StructField("measure_code", IntegerType),
                StructField("seasonal", StringType),
                StructField("srd_code", StringType), 
		            StructField("series_title", StringType),
		            StructField("footnote_codes", StringType), 
                StructField("begin_year", IntegerType),
                StructField("begin_period", StringType),
                StructField("end_year", IntegerType)
  ))	
  val seriesData = spark.read.schema(seriesSchema).
                   option("header", "true").option("delimiter", "\t").
                   csv("../../../../data/BigData/bls/la/la.series")
  
  //nebraska 476
  
                      
//Q9:

  val countyData = spark.read.schema(stateSchema).
                   option("header", "true").option("delimiter", "\t").
                   csv("../../../../data/BigData/bls/la/la.data.64.County")
  
  val geoInfo = spark.read.format("csv").option("header","true").load("../../../../data/BigData/bls/zip_codes_states.csv").
      filter(('state!=="PR") && ('state !=="AK") && ('state !=="HI") ).
          select('latitude.cast("Double"), 'longitude.cast("Double"), 'county)
        
  val countyData2000 = countyData.filter(trim('series_id).endsWith("03") && 'year===2000 && 'period === "M13")
  val updatedData2000 = countyData2000.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(4,15)))
  val dataWithCountyNM2000 = updatedData2000.join(laAreaData, 'series_id === 'area_code).select('area_text, 'value)
  val countyDataWithGeo2000 = dataWithCountyNM2000.join(geoInfo, 'area_text.contains('county)).select('county, 'latitude, 'longitude, 'value).distinct
  val goodData2000 = countyDataWithGeo2000.filter('latitude.isNotNull && 'longitude.isNotNull && 'value.isNotNull)
  val values2000 =  goodData2000.select('value).rdd.map(_(0).asInstanceOf[Double]).collect()
  val latitude2000 = goodData2000.select('latitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  val longitude2000 = goodData2000.select('longitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  
  val countyData2005 = countyData.filter(trim('series_id).endsWith("03") && 'year===2005 && 'period === "M13")
  val updatedData2005 = countyData2005.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(4,15)))
  val dataWithCountyNM2005 = updatedData2005.join(laAreaData, 'series_id === 'area_code).select('area_text, 'value)
  val countyDataWithGeo2005 = dataWithCountyNM2005.join(geoInfo, 'area_text.contains('county)).select('county, 'latitude, 'longitude, 'value).distinct
  val goodData2005 = countyDataWithGeo2005.filter('latitude.isNotNull && 'longitude.isNotNull && 'value.isNotNull)
  val values2005 = goodData2005.select('value).rdd.map(_(0).asInstanceOf[Double]).collect()
  val latitude2005 = goodData2005.select('latitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  val longitude2005 = goodData2005.select('longitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  
  val countyData2010 = countyData.filter(trim('series_id).endsWith("03") && 'year===2010 && 'period === "M13")
  val updatedData2010 = countyData2010.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(4,15)))
  val dataWithCountyNM2010 = updatedData2010.join(laAreaData, 'series_id === 'area_code).select('area_text, 'value)
  val countyDataWithGeo2010 = dataWithCountyNM2010.join(geoInfo, 'area_text.contains('county)).select('county, 'latitude, 'longitude, 'value).distinct
  val goodData2010 = countyDataWithGeo2010.filter('latitude.isNotNull && 'longitude.isNotNull && 'value.isNotNull)
  val values2010 = goodData2010.select('value).rdd.map(_(0).asInstanceOf[Double]).collect()
  val latitude2010 = goodData2010.select('latitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  val longitude2010 = goodData2010.select('longitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  
  val countyData2015 = countyData.filter(trim('series_id).endsWith("03") && 'year===2015 && 'period === "M13")
  val updatedData2015 = countyData2015.withColumn("series_id", regexp_replace(col("series_id"),col("series_id"), col("series_id").substr(4,15)))
  val dataWithCountyNM2015 = updatedData2015.join(laAreaData, 'series_id === 'area_code).select('area_text, 'value)
  val countyDataWithGeo2015 = dataWithCountyNM2015.join(geoInfo, 'area_text.contains('county)).select('county, 'latitude, 'longitude, 'value).distinct
  val goodData2015 = countyDataWithGeo2015.filter('latitude.isNotNull && 'longitude.isNotNull && 'value.isNotNull)
  val values2015 = goodData2015.select('value).rdd.map(_(0).asInstanceOf[Double]).collect()
  val latitude2015 = goodData2015.select('latitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  val longitude2015 = goodData2015.select('longitude).rdd.map(_(0).asInstanceOf[Double]).collect()
  
  val plotSeq2000 = Seq(latitude2000, longitude2000)
  val colorGrad = ColorGradient(4.0 -> RedARGB, 8.0 -> YellowARGB, 12.0 -> GreenARGB, 16.0 -> BlueARGB)
  /*val samplePlot = Plot.scatterPlot(latitude2000, longitude2000, "Unemployment By County in 2000", 
      "Latitude", "Longitude", 5, symbolColor = colorGrad(values2000))
  SwingRenderer(samplePlot, 666,666,true)*/
  
  //the variable name is actually labeled in flip. Oops...
  val gridPlot = Plot.scatterPlotGrid(Seq(
          Seq((longitude2000, latitude2000, colorGrad(values2000), 4), (longitude2005, latitude2005, colorGrad(values2005), 4)),
          Seq((longitude2010, latitude2010, colorGrad(values2010), 4), (longitude2015, latitude2015, colorGrad(values2015), 4))
      ), "Unemployment across continental US in 2000, 2005, 2010 and 2015 (from top left to bottom right in order)",
      "Longitutde", "Latitude")
	SwingRenderer(gridPlot, 1000, 1000, true)
	
	
	
	
  spark.stop()
}
