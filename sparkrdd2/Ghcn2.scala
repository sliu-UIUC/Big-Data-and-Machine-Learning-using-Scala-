package sparkrdd2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import swiftvis2.plotting.styles.ScatterStyle

case class Station (id:String, latitude:Double, longitude:Double, elevation:Double, state:String, name:String, gsnFlag:String, categoryFlag:String, wmoID:String)

case class Data (id:String, time:String, obsType:String, obsValue:Double, mFlag:String, qFlag:String, sFlag:String, obsTime:String)

object Ghcn2 extends App {
	val conf = new SparkConf().setAppName("Ghcnd Data").setMaster("local[*]")
	val sc = new SparkContext(conf)
	sc.setLogLevel("WARN")

	val linesStations = sc.textFile("../../../mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
	val lines2017 = sc.textFile("../../../mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
	val lines1897 = sc.textFile("../../../mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/1897.csv")
        val stationData = linesStations.map { line =>
                Station(line.take(11), line.drop(12).take(8).trim.toDouble, line.drop(21).take(9).trim.toDouble,
                         line.drop(31).take(6).trim.toDouble, line.drop(38).take(2).trim, line.drop(41).take(20).trim,
                         line.drop(72).take(3).trim, line.drop(76).take(3).trim, line.drop(80).take(5).trim
                )
        }
	val yearData2017 = lines2017.map {line =>
                val p = (line++" ").split(",")
                Data(p(0), p(1), p(2),p(3).toDouble, p(4), p(5), p(6), p(7).trim)
        }
	val yearData1897 = lines1897.map {line =>
                val p = (line++" ").split(",")
                Data(p(0), p(1), p(2),p(3).toDouble, p(4), p(5), p(6), p(7).trim)
        }

//********Question 1*********
	val tminANDtmax2017   = yearData2017.filter(x=>x.obsType=="TMAX" || x.obsType=="TMIN")
	val dataWithMinAndMax = tminANDtmax2017.groupBy(x=>(x.id, x.time)).mapValues{x=>x.toList}
	val validDataMinMax   = dataWithMinAndMax.filter(_._2.size==2)
	val maximumDailyDiff  = validDataMinMax.map(x=>x._2(0).obsValue-x._2(1).obsValue).max
	val biggestDiffMinMax = validDataMinMax.filter(x=>x._2(0).obsValue-x._2(1).obsValue==maximumDailyDiff).collect
	val locBigDiffMinMax  = stationData.filter(_.id==biggestDiffMinMax(0)._1._1).collect()(0).name
	println("The largest daily difference in one day is "+maximumDailyDiff) //1252.0 (T-flag)
	println("The station and time that it occurs are: "+biggestDiffMinMax(0)._1) //USS0047P03S, 2017.07.19
	println("The location is: "+locBigDiffMinMax) // Fairbanks F.O., AK, U.S

//********Question 2*********
	//all the tmin values, group by station id, get all the tmin data from 
	// that stations, for each station get the minimum tmin
	val idGroupsMinimum = tminANDtmax2017.filter(_.obsType=="TMIN").groupBy(_.id).mapValues(_.toList.map(_.obsValue)).map{case(a:String,b:List[Double])=>(a, b.min)}
	val idGroupsMaximum = tminANDtmax2017.filter(_.obsType=="TMAX").groupBy(_.id).mapValues(_.toList.map(_.obsValue)).map{case(a:String,b:List[Double])=>(a, b.max)}
	val idGroupsMaxMin = idGroupsMaximum.join(idGroupsMinimum)
	val reverseSortedGrps = idGroupsMaxMin.map{case(a:String, b: (Double, Double)) => (a, b._1-b._2)}.sortBy(_._2, false)
	val largestTempDiff2017 = reverseSortedGrps.take(1)(0) //USS0045R01S,1299.0 (129.9 Celcius)
	val locationName = stationData.filter(_.id==largestTempDiff2017._1).collect()(0).name//Fort Yukon
	println("The largest temperature difference(station, value): "+ largestTempDiff2017)
	println("The location Name is: "+locationName)


//********Question 3*********
	val usStations  = yearData2017.filter(_.id.startsWith("US"))
	val highTempUS  = usStations.filter(_.obsType=="TMAX").map(_.obsValue).collect
	val lowTempUS   = usStations.filter(_.obsType=="TMIN").map(_.obsValue).collect
	val highTempSZ  = highTempUS.size
	val lowTempSZ   = lowTempUS.size
	val avgHighTemp = highTempUS.sum / highTempSZ // 176.35429896742318
	val avgLowTemp  = lowTempUS.sum / lowTempSZ  //55.55885247597211
	val sdHighTemp  = math.pow(highTempUS.map(x=> math.pow((x-avgHighTemp),2)).sum/(highTempSZ-1),0.5) //118.01571248027781
	val sdLowTemp   = math.pow(lowTempUS.map(x=> math.pow((x-avgLowTemp), 2)).sum/(lowTempSZ-1), 0.5) //105.55764819304729
	println("The standard deviation for high temperature for US stations in 2017 are: "+sdHighTemp)
	println("The standard deviation for low temperature for US stations in 2017 are: "+sdLowTemp)


//********Question 4*********
	val stations1897 = yearData1897.map(_.id)
	val stations2017 = yearData2017.map(_.id)
	val intersects   = stations1897.intersection(stations2017).count // 1871
	println("Number of stations that reported data in both 1897 and 2017 is: "+intersects)

//********Question 5********* 

//5a:
	val usStationsList    = stationData.filter(_.id.startsWith("US"))
	val lat0To35Stations  = usStationsList.filter(_.latitude<35).map(_.id).collect
	val lat35To42Stations = usStationsList.filter(s=> s.latitude>35 && s.latitude<42).map(_.id).collect
	val lat42ToStations   = usStationsList.filter(_.latitude>42).map(_.id).collect
	val less35Data2017HT  = yearData2017.filter(d=>lat0To35Stations.contains(d.id) && d.obsType=="TMAX")
	val less42Data2017HT  = yearData2017.filter(d=>lat35To42Stations.contains(d.id) && d.obsType=="TMAX")
  val more42Data2017HT  = yearData2017.filter(d=>lat42ToStations.contains(d.id) && d.obsType=="TMAX")

	val sdLess35Data2017  = less35Data2017HT.map(_.obsValue).stdev //77.4718697979876
	val sdLess42Data2017  = less42Data2017HT.map(_.obsValue).stdev //105.08936590234607 
	val sdMore42Data2017  = more42Data2017HT.map(_.obsValue).stdev //122.58541587072514
	println("The standard deviation for stations with less than 35 latitude: "+sdLess35Data2017)
  println("The standard deviation for stations with 35<latitude<42 : "+sdLess42Data2017)
  println("The standard deviation for stations with more than 42 latitude: "+sdMore42Data2017)

//5b:

	val from0To35Tmin     = yearData2017.filter(d=>lat0To35Stations.contains(d.id) && d.obsType=="TMIN")
	val from35To42Tmin    = yearData2017.filter(d=>lat35To42Stations.contains(d.id) && d.obsType=="TMIN")
  val from42Tmin        = yearData2017.filter(d=>lat42ToStations.contains(d.id) && d.obsType=="TMIN")

	val from0To35HTGrps   = less35Data2017HT.groupBy(x=>(x.id, x.time)).mapValues{_.toList(0).obsValue}
	val from0To35LTGrps   = from0To35Tmin.groupBy(x=>(x.id, x.time)).mapValues{_.toList(0).obsValue}
	val from0To35GrpsHL   = from0To35HTGrps.join(from0To35LTGrps)
	val sdAvgDailyTemp1   = from0To35GrpsHL.map(x=>(x._2._1+x._2._2)/2).stdev //76.46104076820222

	val from35To42HTGrps   = less42Data2017HT.groupBy(x=>(x.id, x.time)).mapValues{_.toList(0).obsValue}
  val from35To42LTGrps   = from35To42Tmin.groupBy(x=>(x.id, x.time)).mapValues{_.toList(0).obsValue}
  val from35To42GrpsHL   = from35To42HTGrps.join(from35To42LTGrps)
  val sdAvgDailyTemp2    = from35To42GrpsHL.map(x=>(x._2._1+x._2._2)/2).stdev //96.71013431970741  

	val from42ToHTGrps     = more42Data2017HT.groupBy(x=>(x.id, x.time)).mapValues{_.toList(0).obsValue}
  val from42ToLTGrps     = from42Tmin.groupBy(x=>(x.id, x.time)).mapValues{_.toList(0).obsValue}
  val from42ToGrpsHL     = from42ToHTGrps.join(from42ToLTGrps)
  val sdAvgDailyTemp3    = from42ToGrpsHL.map(x=>(x._2._1+x._2._2)/2).stdev//109.77906417438031

	println("Standard deviation with lat(0-35): "+sdAvgDailyTemp1)
  println("Standard deviation with lat(35-42): "+sdAvgDailyTemp2)
  println("Standard deviation with lat(42-): "+sdAvgDailyTemp3)


//5c: 
  
	val histData1 = less35Data2017HT.map(_.obsValue).map(_/10)
	//var histDataCollec = histData1.collect()
	//var minHist = histDataCollec.min
	//var maxHist = histDataCollec.max
	//println(minHist)  //-56.1
	//println(maxHist)  //60.0
	val bins1 = (-56.1 to 60.0 by 1.0).toArray 
	val count1 = histData1.histogram(bins1)
	val histPlot1 = Plot.histogramPlot(bins1, count1, RedARGB, false, "High Temp plot (lat<35)", "Temperatures", "Frequency")
	SwingRenderer(histPlot1, 666,666,true)	

	val histData2 = less42Data2017HT.map(_.obsValue).map(_/10)
	//histDataCollec = histData2.collect()
	//minHist = histDataCollec.min  // -60.6
	//maxHist = histDataCollec.max  // 62.0
	//println(minHist)
	//println(maxHist)
	val bins2 = (-60.6 to 62.0 by 1.0).toArray
	val count2 = histData2.histogram(bins2)

	val histPlot2 = Plot.histogramPlot(bins2, count2, GreenARGB, false, "High Temp plot (35<lat<42)", "Temperatures", "Frequency")
  SwingRenderer(histPlot2, 666,666,true)

	val histData3 = more42Data2017HT.map(_.obsValue).map(_/10)
	//histDataCollec = histData3.collect()
	//minHist = histDataCollec.min //-99.9
	//maxHist = histDataCollec.max  //65.6
	//println(minHist)
	//println(maxHist)
	val bins3 = (-99.9 to 65.6 by 1.0).toArray
	val count3 = histData3.histogram(bins3)
  val histPlot3 = Plot.histogramPlot(bins3, count3, BlackARGB, false, "High Temp plot (lat>42)", "Temperatures", "Frequency")
  SwingRenderer(histPlot3, 666,666,true)


//********Question 6*********

	def getStation(input: Iterable[Data]) {
		input.toList(0).id
	}
	def getLongitude(currID:String) = {
		stationData.filter(_.id==currID).collect()(0).longitude
	} 
	def getLatitude(currID:String) = {
                stationData.filter(_.id==currID).collect()(0).latitude
  }
	val obsValDataHT2017   = yearData2017.filter(_.obsType=="TMAX").groupBy(_.id).mapValues{d=>d.toList.map(_.obsValue).sum/(d.toList.size)}
	val latLongDataHT2017  = stationData.map(s=>(s.id, (s.longitude, s.latitude)))
	val ultimatePlotHT2017 = obsValDataHT2017.join(latLongDataHT2017).collect //map values: (avgValue,(longitude, latitude))
	val colorGrad = ColorGradient(-177.8 -> BlueARGB, 100.0 -> GreenARGB, 377.8 -> RedARGB)
  val highTempPlot2017 = Plot.scatterPlot(ultimatePlotHT2017.map(_._2._2._1), ultimatePlotHT2017.map(_._2._2._2),"Average High Temperature 2017 in tenths of Degrees, C", "Longitude", "Latitude", symbolSize = 7,symbolColor = colorGrad(ultimatePlotHT2017.map(_._2._1))) 
	SwingRenderer(highTempPlot2017, 666, 666, true)

//********Question 7*********
//7a: 
	val minMaxStations1897 = yearData1897.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) //must include both TMAX and TMIN
	val minMaxData1897     = minMaxStations1897.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
	val meanMinMax1897     = minMaxData1897.mean //108.51641912973152
 
        val minMaxStations2017 = yearData2017.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) //must include both TMAX and TMIN
        val minMaxData2017     = minMaxStations2017.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax2017     = minMaxData2017.mean //112.89995447511268 

	println("Average temperature in 1897: "+meanMinMax1897)
	println("Average temperature in 2017: "+meanMinMax2017)
//7b: 
	val commonStations     = minMaxStations1897.map(_._1._1).intersection(minMaxStations2017.map(_._1._1)).collect
	val commonData1897     = yearData1897.filter(d=>commonStations.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        
	val commonData2017     = yearData2017.filter(d=>commonStations.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
	val minMaxCommon1897   = commonData1897.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
	val minMaxCommon2017   = commonData2017.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
	val meanCommon1897     = minMaxCommon1897.mean  //109.26912110944664
        val meanCommon2017     = minMaxCommon2017.mean  //119.72067205588897  
	println("Average temperature for common stations in 1897: "+meanCommon1897)
	println("Average temperature for common stations in 2017: "+meanCommon2017)

//7c: 
	def mapData(lines: org.apache.spark.rdd.RDD[String]) = {
		lines.map{ line => 
			val p = (line++" ").split(",")
                	Data(p(0), p(1), p(2),p(3).toDouble, p(4), p(5), p(6), p(7).trim)
        	}
	}
	println("Reading text files...")
	val plotLines1897 = sc.textFile("../../../../data/BigData/ghcn-daily/1897.csv")
        val plotLines1907 = sc.textFile("../../../../data/BigData/ghcn-daily/1907.csv")
        val plotLines1917 = sc.textFile("../../../../data/BigData/ghcn-daily/1917.csv")
        val plotLines1927 = sc.textFile("../../../../data/BigData/ghcn-daily/1927.csv")
        val plotLines1937 = sc.textFile("../../../../data/BigData/ghcn-daily/1937.csv")
        val plotLines1947 = sc.textFile("../../../../data/BigData/ghcn-daily/1947.csv")
        val plotLines1957 = sc.textFile("../../../../data/BigData/ghcn-daily/1957.csv")
        val plotLines1967 = sc.textFile("../../../../data/BigData/ghcn-daily/1967.csv")
        val plotLines1977 = sc.textFile("../../../../data/BigData/ghcn-daily/1977.csv")
        val plotLines1987 = sc.textFile("../../../../data/BigData/ghcn-daily/1987.csv")
        val plotLines1997 = sc.textFile("../../../../data/BigData/ghcn-daily/1997.csv")
        val plotLines2007 = sc.textFile("../../../../data/BigData/ghcn-daily/2007.csv")
        val plotLines2017 = sc.textFile("../../../../data/BigData/ghcn-daily/2017.csv")

	println("Mapping plot data...")
	val plotData1897 = mapData(plotLines1897)
        val plotData1907 = mapData(plotLines1907)
        val plotData1917 = mapData(plotLines1917)
        val plotData1927 = mapData(plotLines1927)
        val plotData1937 = mapData(plotLines1937)
        val plotData1947 = mapData(plotLines1947)
        val plotData1957 = mapData(plotLines1957)
        val plotData1967 = mapData(plotLines1967)
        val plotData1977 = mapData(plotLines1977)
        val plotData1987 = mapData(plotLines1987)
        val plotData1997 = mapData(plotLines1997)
        val plotData2007 = mapData(plotLines2007)
        val plotData2017 = mapData(plotLines2017)

//	println("Handling year inputs...")
	val minMaxStations1907 = plotData1907.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) 
        val minMaxData1907     = minMaxStations1907.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1907     = minMaxData1907.mean //110.10047040971166 
	
      	val minMaxStations1917 = plotData1917.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxData1917     = minMaxStations1917.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1917     = minMaxData1917.mean //96.96104782547863
 
        val minMaxStations1927 = plotData1927.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) 
        val minMaxData1927     = minMaxStations1927.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1927     = minMaxData1927.mean //105.57703099284164

        val minMaxStations1937 = plotData1937.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) 
        val minMaxData1937     = minMaxStations1937.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1937     = minMaxData1937.mean //99.71359797720632 

        val minMaxStations1947 = plotData1947.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) 
        val minMaxData1947     = minMaxStations1947.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1947     = minMaxData1947.mean //103.22160109845474

        val minMaxStations1957 = plotData1957.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxData1957     = minMaxStations1957.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1957     = minMaxData1957.mean //109.26347594528664

        val minMaxStations1967 = plotData1967.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) 
        val minMaxData1967     = minMaxStations1967.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1967     = minMaxData1967.mean //102.87190751727877

      	val minMaxStations1977 = plotData1977.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxData1977     = minMaxStations1977.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1977     = minMaxData1977.mean //102.86139410480388

        val minMaxStations1987 = plotData1987.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) 
        val minMaxData1987     = minMaxStations1987.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1987     = minMaxData1987.mean //103.28413056821799

        val minMaxStations1997 = plotData1997.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxData1997     = minMaxStations1997.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax1997     = minMaxData1997.mean //100.8233369690002

        val minMaxStations2007 = plotData2007.filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2) 
        val minMaxData2007     = minMaxStations2007.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanMinMax2007     = minMaxData2007.mean //108.70107726228748 

	val years              =  Array(1897, 1907, 1917, 
				        1927, 1937, 1947, 
				        1957, 1967, 1977, 
				        1987, 1997, 2007,
					2017)

	val yearlyTempData     =  Array(108.51641912973152, 110.10047040971166, 96.96104782547863,
				                          105.57703099284164, 99.71359797720632, 103.22160109845474,
					                        109.26347594528664, 102.87190751727877,102.86139410480388,
					                        103.28413056821799, 100.8233369690002, 108.70107726228748,
					                        112.89995447511261)
	val yearlyTempPlot     = Plot.scatterPlotWithLines(years, yearlyTempData.map(_/10), "Yearly Average Temperature for all stations(at that year) from 1897 to 2017",
	    "Year", "Temperature (Celcius)", symbolSize = 5,
				 symbolColor = GreenARGB)
	SwingRenderer(yearlyTempPlot, 666,666)
//7d: 
	var stationArr:Array[org.apache.spark.rdd.RDD[String]] = Array()
	val yearsArr = Array(minMaxStations1897, minMaxStations1907, minMaxStations1917, minMaxStations1927,
			     minMaxStations1937, minMaxStations1947, minMaxStations1957, minMaxStations1967,
			     minMaxStations1977, minMaxStations1987, minMaxStations1997, minMaxStations2007,
			     minMaxStations2017)

	for(i <- 0 to yearsArr.size-1) {
		stationArr = stationArr :+ yearsArr(i).map(_._1._1)
	}
	val stationSeq     = stationArr.toSeq
	var commonStats = stationSeq(0) 
	for(i <- 1 to stationArr.size-1) {
        	commonStats = commonStats.intersection(stationArr(i))
        } //497 common stations 
	val commons = commonStats.collect()
	val common1897    = plotData1897.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
	val minMaxCom1897 = common1897.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
	val meanCom1897   = minMaxCom1897.mean //107.76597717681362
	
        val common1907    = plotData1907.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1907 = common1907.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1907   = minMaxCom1907.mean //105.34320855236807 

        val common1917    = plotData1917.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1917 = common1917.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1917   = minMaxCom1917.mean //97.37103675039457 

        val common1927    = plotData1927.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1927 = common1927.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1927   = minMaxCom1927.mean //111.62712370190584 

        val common1937    = plotData1937.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1937 = common1937.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1937   = minMaxCom1937.mean //108.55474892767968 

        val common1947    = plotData1947.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1947 = common1947.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1947   = minMaxCom1947.mean //109.91120287497381 

        val common1957    = plotData1957.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1957 = common1957.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1957   = minMaxCom1957.mean //111.66298589553243  

        val common1967    = plotData1967.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1967 = common1967.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1967   = minMaxCom1967.mean //107.3229678506247

        val common1977    = plotData1977.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1977 = common1977.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1977   = minMaxCom1977.mean //111.50816814956634 

        val common1987    = plotData1987.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1987 = common1987.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1987   = minMaxCom1987.mean //116.50551693981623 

        val common1997    = plotData1997.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom1997 = common1997.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom1997   = minMaxCom1997.mean //107.10797722883228 

        val common2007    = plotData2007.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom2007 = common2007.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom2007   = minMaxCom2007.mean //115.05927586664589

        val common2017    = yearData2017.filter(d=>commons.contains(d.id)).filter(d=>d.obsType=="TMAX" || d.obsType=="TMIN").groupBy(d=>(d.id, d.time)).filter(d=>d._2.toList.length==2)
        val minMaxCom2017 = common2017.map(_._2.toList).map(x=>(x(0).obsValue+x(1).obsValue)/2)
        val meanCom2017   = minMaxCom2017.mean //119.67294639355637

	      val commonTempData = Array(107.76597717681362, 105.34320855236807, 97.37103675039457, 
	                           111.62712370190584, 108.55474892767968,  109.91120287497381, 
	                           111.66298589553243, 107.3229678506247, 111.50816814956634, 
	                           116.50551693981623, 107.10797722883228, 115.05927586664589, 
	                           119.67294639355637)
	      //Plot.scatterPlotWithLines(x, y, title, xLabel, yLabel, symbolSize, symbolColor, lineGrouping, lineStyle, xType, yType)
	      val yearlyCommonPlot = Plot.scatterPlotWithLines(years, commonTempData.map(_/10), "Yearly Average Temperture for common stations from 1897 to 2017",
	          "Year","Temperature (Celcius)",symbolSize = 5, symbolColor = RedARGB, lineGrouping = 1)
	      SwingRenderer(yearlyCommonPlot, 666,666)              

}
