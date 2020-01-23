//Shiyu, Pledged
package basicscala

import scala.io.Source
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.styles.HistogramStyle

case class eduData(locID:Int, locCode: String, locName: String, year:Int, ageGrpID:Int,
	ageGrpName:String, sexID:Int, sexName: String, metric: String, unit:String,
	mean:Double, upper:Double, lower:Double)

object eduData extends App {
	def parseLine(line:String):eduData = {
		val p = line.split(",")
		if(p.length == 13) {
			eduData(p(0).toInt, p(1), p(2), p(3).toInt, p(4).toInt, 
				p(5), p(6).toInt, p(7), p(8), p(9), 
				p(10).toDouble, p(11).toDouble, p(12).toDouble
			)
		} else {
			eduData(p(0).toInt, p(1), p(2)+","+p(3)+","+p(4), p(5).toInt, p(6).toInt,
                                p(7), p(8).toInt, p(9), p(10), p(11),
                                p(12).toDouble, p(13).toDouble, p(14).toDouble)
		}
	}
	val sourceEdu = Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV")
	val types = sourceEdu.getLines.next()
	val lines = sourceEdu.getLines
	val data = lines.map(parseLine).toArray
	sourceEdu.close()
	val dataEdu = data.filter(_.metric=="Education Per Capita")
	val highestMeanEdu = dataEdu.maxBy(_.mean) //max by lower and upper
						//yields the same entry
	//val highestLowEdu = dataEdu.maxBy(_.lower)
	//val highestHighEdu = dataEdu.maxBy(_.upper)
	val sortedRankOfIncrease = dataEdu.groupBy(t => (t.locName, t.ageGrpName, t.sexName)).mapValues(x=> x.maxBy(_.mean).mean - x.minBy(_.mean).mean).toSeq.sortBy(_._2)
	val biggestInc = dataEdu.groupBy(t => (t.locName, t.ageGrpName, t.sexName)).mapValues(x=> x.maxBy(_.upper).upper - x.minBy(_.lower).lower).toSeq.sortBy(_._2)

	val largestInc = sortedRankOfIncrease(sortedRankOfIncrease.size-1)
	
	val codeGradEdu = ColorGradient(6.0 -> RedARGB, 102.0 -> BlueARGB, 67.0 -> BlackARGB)	
	val threeCountryData = dataEdu.filter(c=> (c.locName=="China" || c.locName=="United States" || c.locName=="Japan") && c.ageGrpName=="25 to 34" && c.sexName =="Females")
	val eduPlot = Plot.scatterPlot(threeCountryData.map(_.year), threeCountryData.map(_.mean), "educational attainment of females ages 25-34", "Year", "education attainment", symbolColor = codeGradEdu(threeCountryData.map(_.locID)))
	SwingRenderer(eduPlot, 666,666, true)

/***************Answers*************/
	println("--Number of types in education file: "+ types.split(",").length)
	println("	What are they: " + types+"\n")
	println("--Entry with highest mean years of education attainment per capital: " + highestMeanEdu+"\n")
	println("--Largest increase in mean education per capital is: "+ largestInc+"\n")
	println("--Largest different between lowest and highest is: "+biggestInc(biggestInc.size-1))
}
