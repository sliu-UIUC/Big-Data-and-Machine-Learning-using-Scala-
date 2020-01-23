package scalaintro

import scala.io.Source
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.styles.HistogramStyle

case class TempData(day:Int, doy: Int, month: Int, year: Int, precip: Double, tave: Double, tmax: Double, tmin: Double)

object TempData extends App {
	def parseLine(line: String): TempData = {
		val p = line.split(",")
		TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, p(5).toDouble,
			p(6).toDouble, p(7).toDouble, p(8).toDouble)
	}

	val source = Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/SanAntonioTemps.csv")
	val lines = source.getLines.drop(2)
	val data = lines.map(parseLine).toArray
	source.close()

	val hotDay = data.maxBy(_.tmax)
	println(hotDay)
	val wetDay = data.maxBy(_.precip)
	println(wetDay)
	val fracWet = data.count(_.precip >= 1.0).toDouble/data.length
	println(fracWet)
	println(data.map(_.tmax).sum/data.length)
	val rainyDays = data.filter(_.precip > 0.0)
	val reallyRainyDays = data.filter(_.precip >= 1.0)
	val averageTempOnRainyDays = rainyDays.foldLeft(0.0)((acc, td) => acc+td.tmax)/rainyDays.length
	val averageTempOnReallyRainyDays = reallyRainyDays.foldLeft(0.0)((acc, td) => acc+td.tmax)/reallyRainyDays.length
	println(averageTempOnRainyDays)
	println(averageTempOnReallyRainyDays)
	val monthAverages = data.groupBy(_.month).mapValues(days => days.map(_.tmax).sum/days.size)
	monthAverages.toSeq.sortBy(_._1).foreach(println)
	val monthMedians = data.groupBy(_.month).mapValues { days =>
	  val sortedDays = days.sortBy(_.tmax)
	  sortedDays(days.length/2).tmax
	}
	monthMedians.toSeq.sortBy(_._1).foreach(println)
	
	val fullPlot = Plot.scatterPlotWithLines(data.map(td => td.year+td.doy/365.0), data.map(_.tmax), 
	    "Full Data", "Year", "Temperature", 3, lineGrouping = 0)
	SwingRenderer(fullPlot, 800, 800, true)
	
	val yearGradient = ColorGradient(1945.0 -> BlackARGB, 1960.0 -> GreenARGB, 1985.0 -> BlueARGB, 2014.0 -> RedARGB)
	val doyPlot = Plot.scatterPlot(data.map(_.doy), data.map(_.tmax), symbolSize=5,
	    symbolColor = yearGradient(data.map(_.year)))
	SwingRenderer(doyPlot, 800, 800, true)
	
	val dailyAverage = data.groupBy(_.doy).mapValues(days => days.map(_.tmax).sum/days.length).toArray
	val dailyPlot = Plot.scatterPlot(dailyAverage.map(_._1), dailyAverage.map(_._2))
	SwingRenderer(dailyPlot, 800, 800, true)
	
	val tempHist = Plot.histogramPlotFromData(0 to 111, data.map(_.tmax), 0x88778822, "Temp Hist", "Temperatures", "Frequency")
	//SwingRenderer(tempHist, 800, 800, true)
	
	// Alex's fault
	val hists = for(year <- 1940 to 2010 by 10) yield {
	  val d = data.filter(td => td.year >= year && td.year < year+10) 
	  HistogramStyle.fromData(d.map(_.tmax), 0 to 111, 0x22223355, true)
	}
	val histStack = Plot.stacked(hists, "Alex's Plot", "Temps", "Frequency")
	SwingRenderer(histStack, 800, 800, true)
	
	// 4-D plot
	val size = data.map(_.precip+4)
	val tempGrad = ColorGradient(40.0 -> BlueARGB, 75.0 -> GreenARGB, 110.0 -> RedARGB)
	val fourDPlot = Plot.scatterPlot(data.map(_.year), data.map(_.doy), 
	    "We don't know", "Year", "Day of Year", symbolSize = size, 
	    symbolColor = tempGrad(data.map(_.tmax)))
	SwingRenderer(fourDPlot, 800, 800, true)
	
	val words = "This is a test of your coding ability.".split(" ")
	val chars = words.foldLeft(0)((acc, w) => acc+w.length)

}
