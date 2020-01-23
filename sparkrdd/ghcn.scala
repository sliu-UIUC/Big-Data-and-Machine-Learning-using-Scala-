import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import swiftvis2.plotting.styles.ScatterStyle


case class Countries (code:String, name: String)

case class Stations (id:String, latitude:Double, longitude:Double, elevation:Double, state:String, name:String, gsnFlag:String, categoryFlag:String, wmoID:String)

case class Data2017 (id:String, time:String, obsType:String, obsValue:String, mFlag:String, qFlag:String, sFlag:String, obsTime:String)

object SparkRDD extends App {
	val conf = new SparkConf().setAppName("Ghcnd Data").setMaster("local[*]")
	val sc = new SparkContext(conf)
  
	sc.setLogLevel("WARN")
  	
	val linesStations = sc.textFile("../../../mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
	val linesCountries = sc.textFile("../../../mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-countries.txt")
        val lines2017 = sc.textFile("../../../mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
	val stationData = linesStations.map { line => 
		Stations(line.take(11), line.drop(12).take(8).trim.toDouble, line.drop(21).take(9).trim.toDouble,
			 line.drop(31).take(6).trim.toDouble, line.drop(38).take(2).trim, line.drop(41).take(20).trim,
			 line.drop(72).take(3).trim, line.drop(76).take(3).trim, line.drop(80).take(5).trim
		)
	}
	val countryData = linesCountries.map { line => 
		val p = line.split(' ')
		Countries(p(0), p.drop(1).mkString(" ").trim)
	}
	val yearData2017 = lines2017.map {line => 
		val p = (line++" ").split(",")
		Data2017(p(0), p(1), p(2),p(3), p(4), p(5), p(6), p(7).trim)
	}

//********Question 1********
	val texasStations = stationData.filter(_.state=="TX")
	println("\n The number of stations in Texas are: " + texasStations.count()+".\n") //4736


//********Question 2********
	val year2017Stations = yearData2017.map(_.id).distinct.collect()
	val txStations2017 = texasStations.map(_.id).filter(d=>year2017Stations.contains(d)).count()
	println("Number of those stations that reported some data: "+ txStations2017 + "\n")   //2487


//********Question 3********
	val tmaxData = yearData2017.filter(_.obsType=="TMAX")
	val highestTmax = tmaxData.map(_.obsValue.toInt).max
	val highestTmaxEntry = tmaxData.filter( _.obsValue.toInt == highestTmax).collect()(0)//there is only one entry with the highestTmax value
	val locationHighestTmax = countryData.filter(_.code==highestTmaxEntry.id.take(2)).collect()(0).name
	val stationLoc = stationData.filter(_.id==highestTmaxEntry.id).collect()(0).name
	println("The highest temperature is: "+highestTmax+" (in tenths of degrees C )") //656, United States
	println("The location of this observation is: "+stationLoc)
	println("The observation time for the highest temperature is : " + highestTmaxEntry.time +" "+highestTmaxEntry.obsTime+"\n")

//********Question 4*********
	val stationList = stationData.map(_.id)	.collect
	val numInactiveStations = stationList.filterNot(d=> year2017Stations.contains(d)).length //66449
	println("Number of stations in the station list that didnt report any data in 2017: " + numInactiveStations + "\n")


//********Question 5*********
	val txStationData = texasStations.map(_.id).collect
        val activeTxStationsPRCP = yearData2017.filter(d => txStationData.contains(d.id) && d.obsType=="PRCP")
	//val rainfallRanking = activeTxStationsPRCP.sortBy(_.obsValue,false)
	val maxRainfall = activeTxStationsPRCP.map(_.obsValue.toDouble).max() 
	//val maxRainfall = rainfallRanking.take(1)
	val maxRainfallEntries = activeTxStationsPRCP.filter(_.obsValue==maxRainfall).collect()
	println("Maximum rainfall by any station in Texas in 2017 is: "+maxRainfall + "\n") //6612
	println(maxRainfallEntries(0))


//********Question 6********
	val indiaCode = countryData.filter(_.name=="India").collect.head.code
	val indiaPRCPStations = yearData2017.filter(d => d.id.take(2)==indiaCode && d.obsType=="PRCP")//4361
	val maxRainfallIN = indiaPRCPStations.map(_.obsValue.toDouble).max() 
	val maxRainfallEntriesIN = indiaPRCPStations.filter(_.obsValue==maxRainfallIN).collect()
	println("Maxmimum rainfall in india in 2017 is: "+ maxRainfallIN +"\n")
	println(maxRainfallEntriesIN(0))

//********Question 7********(have issues)
	val saStationEntries = texasStations.filter(_.name.contains("SAN ANTONIO"))
	println("There are "+saStationEntries.count+" weather stations in San Antonio."+"\n")
	//30

//********Question 8********
//TMIN, TMAX, TAVG, TOBS
	val saStations = saStationEntries.map(_.id).collect
	val activeSAstationEntries = yearData2017.filter(d=> saStations.contains(d.id) && 
		(d.obsType=="TMIN" || d.obsType=="TMAX" || d.obsType=="TAVG" || d.obsType=="TOBS")
	)
	val numActiveSAstations = activeSAstationEntries.map(_.id).distinct.count
        println("There are "+numActiveSAstations+" weather stations in SA that reported temperature data in 2017."+"\n") //4


//********Question 9********
	val saHighTempData = yearData2017.filter(d=> saStations.contains(d.id) && d.obsType=="TMAX")
	val grpLst = saHighTempData.groupBy(_.id).mapValues {v=>v.toList.sortBy(_.time)}.collect
	val tempLst = grpLst.map(_._2).map(_.toArray).map(_.map(d=>d.obsValue.toInt))
	val tempLstLag1Day = tempLst.map(_.drop(1))
	val maxDiffByStations = tempLst.map(a=>a.zip(tempLstLag1Day(tempLst.indexOf(a))))map(_.map(a=>a._2-a._1).max)
	val maxDiff = maxDiffByStations.max
	println("The biggest daily temperature jump is: " + maxDiff + "\n") //122
	

//********Question 10*******
	//S.D highest temp, S.D rainfall, COV(high temp, rainfall)
	val saRainfallDataset = yearData2017.filter(d=> saStations.contains(d.id) && d.obsType=="PRCP").collect.toSet
	val saHighTempDataset = saHighTempData.collect.toSet
	val commonStationNtime = saRainfallDataset.map(a=>(a.id, a.time)).intersect(saHighTempDataset.map(a=>(a.id, a.time)))
	//below are the data shared same date & station in PRCP and TMAX(sorted)
	val saRainfalls = saRainfallDataset.filter(a=>commonStationNtime.contains((a.id,a.time))).toArray.sortBy(_.id).sortBy(_.time)
	val saHighTemps = saHighTempDataset.filter(a=>commonStationNtime.contains((a.id,a.time))).toArray.sortBy(_.id).sortBy(_.time)
	//below are code to calculate standard deviations for rainfall and high temp
	val rainfallValues = saRainfalls.map(_.obsValue.toDouble)
	val highTempValues = saHighTemps.map(_.obsValue.toDouble)
	val num            = rainfallValues.length
	val rainfallMean   = (rainfallValues.sum)/num
	val highTempMean   = (highTempValues.sum)/num
	val rainfallSD     = math.pow(rainfallValues.map(a=> math.pow(a-rainfallMean,2)).sum/num, 0.5)
	val highTempSD     = math.pow(highTempValues.map(a=> math.pow(a-highTempMean,2)).sum/num, 0.5) 
	val covariance     = rainfallValues.map(_-rainfallMean).zip(highTempValues.map(_-highTempMean)).map(a=>a._1*a._2).sum/num
	val correlationCoef= covariance/(rainfallSD*highTempSD)    //-0.14664598765512762
	println("The correlation coefficient is: " + correlationCoef+"\n")

//********Question 11*******
	//WE WILL BE USING TAVG IN OUR GRAPH
	//The five stations are: CHM00054857(36.067N), AE000041196(25.3333N),AYM00089022(-75.450S), FRE00104937(49.7253N), USR0000AALC(62.8176N)
	//Those locations are China(Qingdao,northern east coast), United Arab Emirates(Sharjah International Airport),
	// France (La Hague) Antarctica (Halley station) and United States (Alaska)
	val CHData = yearData2017.filter(d=>d.id=="CHM00054857" && d.obsType=="TAVG").collect
	val AEData = yearData2017.filter(d=>d.id=="AE000041196" && d.obsType=="TAVG").collect
	val AYData = yearData2017.filter(d=>d.id=="AYM00089022" && d.obsType=="TAVG").collect
	val FEData = yearData2017.filter(d=>d.id=="FRE00104937" && d.obsType=="TAVG").collect
	val USData = yearData2017.filter(d=>d.id=="USR0000AALC" && d.obsType=="TAVG").collect
	//val totData= CHData++AEData++AYData++USData++FEData
	
	//val latGrd = ColorGradient(36.067-> RedARGB, 25.3333 -> YellowARGB, -75.450 -> BlueARGB, 49.7253 ->BlackARGB, 62.8176 -> GreenARGB)
	//val plot   = Plot.scatterPlot(totData.map(_.time.toDouble), totData.map(_.obsValue.toDouble),"Average Temperature changes in 5 stations (2017)", "Date", "Temperature in Celcius")
	val dayRange = 1 to 365
	val CHplot = ScatterStyle(dayRange,CHData.map(_.obsValue.toDouble), colors = RedARGB)
	val AEplot = ScatterStyle(dayRange,AEData.map(_.obsValue.toDouble), colors = YellowARGB)
	val AYplot = ScatterStyle(dayRange,AYData.map(_.obsValue.toDouble), colors = BlueARGB)
	val FEplot = ScatterStyle(dayRange,FEData.map(_.obsValue.toDouble), colors = BlackARGB)
	val USplot = ScatterStyle(dayRange,USData.map(_.obsValue.toDouble), colors = GreenARGB)
	val plot   = Plot.stacked(Array(CHplot, AEplot, AYplot, FEplot, USplot),"Average temperature for 5 stations in 2017","Day", "Average Temperature (measured in tenths of degrees Celcius")
	SwingRenderer(plot,666,666,true)

  	sc.stop()
}
