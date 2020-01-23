//Shiyu, Pledged
package basicscala

import scala.io.Source
import basicscala.eduData
import basicscala.gdpData
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.styles.HistogramStyle


case class locData(countryCode:String,latitude:Double, longitude:Double, countryName:String)

object plotForQ8AndQ9 extends App {	
        def parseLineEdu(line:String):eduData = {
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

	def parseLineGDP(line:String):gdpData = {
                val p = line.split(",")
                gdpData(p(0), p(1), p(2), p(3), p(4), p(9), p(14),
                        p(19), p(24), p(29), p(34), p(39), p(44),
                        p(49), p(54), p(59))
        }
	
	def parseLineLoc(line:String):locData = {
		val p = line.stripLineEnd.split("\t",-1)
		if(p(0)!="UM"){
			val country = p.drop(3).foldRight(" ")((acc,a)=>acc++" "++a).trim
			locData(p(0), p(1).toDouble, p(2).toDouble, country)
		} else {
			val country = p.drop(1).foldRight(" ")((acc,a)=>acc++" "++a).trim
                        locData(p(0), 0.0, 0.0, country)

		}
	}
	val sourceEdu = Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV")
        val linesEdu = sourceEdu.getLines.drop(1)
        val dataEdu = linesEdu.map(parseLineEdu).toArray
        sourceEdu.close()

	val sourceGDP = Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/API_NY.GDP.PCAP.KD_DS2_en_csv_v2_10081022.csv")
        val linesGDP = sourceGDP.getLines.drop(5)
        val dataGDP = linesGDP.map(parseLineGDP).toArray
        sourceGDP.close()

	val sourceLoc = Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/countries.tsv")
	val linesLoc = sourceLoc.getLines.drop(1)
	val dataLoc = linesLoc.map(parseLineLoc).toArray
	sourceLoc.close()

	val youngMaleEduData1970 = dataEdu.filter(x=>x.ageGrpName=="25 to 34" && x.sexName=="Males" && x.metric=="Education Per Capita" && x.year==1970)
	val countrysGDP1970 = dataGDP.filter(_.ninteenSeventy.contains('.')).map(_.ninteenSeventy.filterNot(_=='\"').toDouble)
	val plotMaleGDP1970 = Plot.scatterPlot(countrysGDP1970, youngMaleEduData1970.map(_.mean),"GDP/capita vs education/capita for 25-34 years old Males in 1970", "GDP per capita","Education attainment per capita")
	SwingRenderer(plotMaleGDP1970, 666,666,true)
	val youngFemaleEduData1970 = dataEdu.filter(x=>x.ageGrpName=="25 to 34" && x.sexName=="Females" && x.metric=="Education Per Capita" && x.year==1970) 
	val plotFemaleGDP1970 = Plot.scatterPlot(countrysGDP1970, youngFemaleEduData1970.map(_.mean),"GDP/capita vs education/capita for 25-34 years old Females in 1970", "GDP per capita","Education attainment per capita")
	SwingRenderer(plotFemaleGDP1970, 666,666,true)
	
	val youngMaleEduData2015 = dataEdu.filter(x=>x.ageGrpName=="25 to 34" && x.sexName=="Males" && x.metric=="Education Per Capita" && x.year==2015)
        val countrysGDP2015 = dataGDP.filter(_.twoK15.contains('.')).map(_.twoK15.filterNot(_=='\"').toDouble)
	val plotMaleGDP2015 = Plot.scatterPlot(countrysGDP2015, youngMaleEduData2015.map(_.mean),"GDP/capita vs education/capita for 25-34 years old Males in 2015", "GDP per capita","Education attainment per capita")
        SwingRenderer(plotMaleGDP2015, 666,666,true)

	val youngFemaleEduData2015 = dataEdu.filter(x=>x.ageGrpName=="25 to 34" && x.sexName=="Females" && x.metric=="Education Per Capita" && x.year==2015)
	val plotFemaleGDP2015 = Plot.scatterPlot(countrysGDP2015, youngFemaleEduData2015.map(_.mean),"GDP/capita vs education/capita for 25-34 years old Females in 2015", "GDP per capita","Education attainment per capita")
        SwingRenderer(plotFemaleGDP2015, 666,666,true)
	
	val size1970 = dataGDP.filter(_.ninteenSeventy.contains('.')).map(x=>(x.ninteenSeventy.filterNot(_=='\"').toDouble/700)+2)
	val femaleEdu25to34in1970 = dataEdu.filter(x=> x.year==1970 && x.sexName=="Females" && x.ageGrpName=="25 to 34" && x.metric=="Education Per Capita")
	val eduGrad = ColorGradient(5.0 -> BlueARGB, 10.0 -> GreenARGB, 15.0 -> RedARGB)
	val finalPlot1970 = Plot.scatterPlot(dataLoc.map(_.longitude),dataLoc.map(_.latitude),
		"Education and GDP per capita for female(age 25-34) on different locations(1970)", "Longitude","Latitude",
		symbolSize = size1970,symbolColor = eduGrad(femaleEdu25to34in1970.map(_.mean)))
	SwingRenderer(finalPlot1970,666,666,true)

	val size2015 = dataGDP.filter(_.twoK15.contains('.')).map(x=>(x.twoK15.filterNot(_=='\"').toDouble/700)+2)
        val femaleEdu25to34in2015 = dataEdu.filter(x=> x.year==2015 && x.sexName=="Females" && x.ageGrpName=="25 to 34" && x.metric=="Education Per Capita")
        val eduGrad2015 = ColorGradient(5.0 -> BlueARGB, 10.0 -> GreenARGB, 15.0 -> RedARGB)
        val finalPlot2015 = Plot.scatterPlot(dataLoc.map(_.longitude),dataLoc.map(_.latitude),
                "Education and GDP per capita for females(age 25-34)) on different locations (2015)", "Longitude","Latitude",
                symbolSize = size2015,symbolColor = eduGrad2015(femaleEdu25to34in2015.map(_.mean)))
        SwingRenderer(finalPlot2015,666,666,true)

}
