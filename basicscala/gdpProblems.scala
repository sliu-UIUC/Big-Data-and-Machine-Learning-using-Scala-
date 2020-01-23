//Shiyu, Pledged
package basicscala

import scala.io.Source
import basicscala.eduData
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.ColorGradient

case class gdpData(countryName:String, countryCode:String, idName: String, idCode:String,
	ninteenSixty:String, ninteenSixty5:String, ninteenSeventy:String, ninteenSeventy5:String, 
	ninteenEighty:String, ninteenEighty5:String, ninteenNinty:String, ninteenNinty5:String,
	twoK:String, twoK5: String, twoK10:String, twoK15:String)

object gdpData extends App {
	def parseLine(line:String):gdpData = {
		val p = line.split(",")
		gdpData(p(0), p(1), p(2), p(3), p(4), p(9), p(14), 
			p(19), p(24), p(29), p(34), p(39), p(44), 
			p(49), p(54), p(59))
	}	
	val source = Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/API_NY.GDP.PCAP.KD_DS2_en_csv_v2_10081022.csv")
	val lines = source.getLines.drop(5)
	val dataGDP = lines.map(parseLine).toArray
	source.close()
	
	val validData1970 = dataGDP.filter(_.ninteenSeventy.contains('.'))  //countries with non-empty data in 1970
	val maxGDP1970 = validData1970.maxBy(_.ninteenSeventy.filterNot(_=='\"').toDouble)
	val minGDP1970 = validData1970.minBy(_.ninteenSeventy.filterNot(_=='\"').toDouble)

	val validData2015 = dataGDP.filter(_.twoK15.contains('.'))
	val maxGDP2015 = validData2015.maxBy(_.twoK15.filterNot(_=='\"').toDouble)
	val minGDP2015 = validData2015.minBy(_.twoK15.filterNot(_=='\"').toDouble)

	val validData1970n2015 = dataGDP.filter(d => d.twoK15.contains('.') && d.ninteenSeventy.contains('.'))
	val largestIncrease = validData1970n2015.maxBy(c => c.twoK15.filterNot(_=='\"').toDouble - c.ninteenSeventy.filterNot(_=='\"').toDouble).countryName
	
	val countries = dataGDP.filter(c => c.countryName=="\"China\"" || c.countryName =="\"United States\"" || c.countryName=="\"Japan\"")
	val gdpData3 = countries.map { a=> Array((1960.0,a.ninteenSixty),(1965.0,a.ninteenSixty5), (1970.0, a.ninteenSeventy),
			(1975.0, a.ninteenSeventy5), (1980.0,a.ninteenEighty),(1985.0,a.ninteenEighty5), (1990.0,a.ninteenNinty),
			(1995.0, a.ninteenNinty5), (2000.0,a.twoK),(2005.0, a.twoK5), (2010.0,a.twoK10), (2015.0,a.twoK15))
	}.foldRight(Array[(Double,String)]())((acc,x)=>acc++x)

	val gdpPlot = Plot.scatterPlot(gdpData3.map(_._1.toDouble), gdpData3.map(_._2.filterNot(x=>x=='\"').toDouble),
			"GDP changes for China, the U.S and Japan from 1960 to 2015", "Year", "GDP per capital")
	SwingRenderer(gdpPlot, 666,666,true)
	//println(gdpData3.toSeq(0))

//***********************Answers*************************	
	println("country with largest GDP per capita in 1970 is : " + maxGDP1970.countryName 
		+ "; its GDP per capita is: "+ maxGDP1970.ninteenSeventy)
	println("country with smallest GDP per capita in 1970 is : " + minGDP1970.countryName 
		+ "; its GDP per capita is :" + minGDP1970.ninteenSeventy)
	println("country with largest GDP per capita in 2015 is : " + maxGDP2015.countryName 
		+ "; its GDP per capita is:"+ maxGDP2015.twoK15)
        println("country with smallest GDP per capita in 2015 is : " + minGDP2015.countryName 
		+ ";its GDP per capita is:" + minGDP2015.twoK15)
	println("Country  that had the largest increase in GDP per capital from 1970 to 2015 is: "
		+ largestIncrease)
}
