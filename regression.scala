package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrameStatFunctions

case class Columns(startIdx:Int, fieldName: String, fieldLen:Int)

object regression extends App {
	val spark = SparkSession.builder().master("local[*]").getOrCreate()
  	import spark.implicits._
	spark.sparkContext.setLogLevel("WARN")

	def getColumns(file:String): Array[Columns] = {
		val source   = scala.io.Source.fromFile(file)
		val colLines = source.getLines().drop(1).toArray
		val columns  = colLines.map(_.split('\t')).map(line=> Columns(line(0).toInt, line(1).toString, line(2).toInt))
		source.close()
		columns
	}
	
	val columns  = getColumns("/data/BigData/brfss/Columns.txt")
	val rdds     = spark.sparkContext.textFile("/data/BigData/brfss/LLCP2016.asc")
	val structRdd  = rdds.map{ line=> 	
		val tmp = for(i <- columns) yield {
			val fieldNm = line.substring(i.startIdx, i.startIdx+i.fieldLen).trim()
			try {fieldNm.toDouble} 
			catch{ case e: NumberFormatException => -0.001}
		}
		Row.fromSeq(tmp)
	}

	val schema     = StructType(for(i <- columns) yield {StructField(i.fieldName, DoubleType )})
	val data       = spark.createDataFrame(structRdd, schema)

//*********Question 1************
	val listFields = Array("GENHLTH", "PHYSHLTH", "MENTHLTH", "POORHLTH", "EXERANY2", "SLEPTIM1")
	/*val goodData   = data.filter('GENHLTH <=5 && 'GENHLTH >0 && 'PHYSHLTH <=30 && 'PHYSHLTH>0 )
                                   && 'MENTHLTH <=30 && 'MENTHLTH>0 &&  'POORHLTH<=30 && 'HLTHPLN1<=2
				   && 'PERSDOC2<=3 && 'MEDCOST<=2 && 'CHECKUP1<=4  && 'EXERANY2<=2 
				   && 'SLEPTIM1<=24 && 'CVDINFR4<=2 && 'CVDCRHD4<=2 && 'CVDSTRK3<=2
				   && 'ASTHMA3<=2 && 'CHCOCNCR<=2 && 'CHCCOPD1<=2 && 'HAVARTH3<=2 
				   && 'ADDEPEV2<=2 && 'CHCKIDNY<=2 && 'DIABETE3<=2 && 'EMPLOY1<=8 
				   && 'CHILDREN <=88 && 'INCOME2<=8) */
	for (i <- listFields) {
		data.select(i).describe().show()
	}

//*********Question 2 & 3************
	
	val goodFields = Array("HLTHPLN1", "PERSDOC2","MEDCOST", "CHECKUP1", "EXERANY2", "SLEPTIM1",
				"CVDINFR4", "CVDCRHD4", "CVDSTRK3", "ASTHMA3", "CHCOCNCR", 
				"CHCCOPD1", "HAVARTH3", "ADDEPEV2", "CHCKIDNY", "DIABETE3","EMPLOY1",
				"CHILDREN", "INCOME2", "VETERAN3", "_PHYS14D", "_RFHLTH", //)
				"GENHLTH",/* "PHYSHLTH",*/ "MENTHLTH", "POORHLTH")
	//val bestFields = Array("CHECKUP1","SLEPTIM1", "CHCCOPD1", "HAVARTH3", "INCOME2")
	val bestFields = Array("_RFHLTH", "_PHYS14D")
	def correlation(fieldName:String) = {
		for(i <- goodFields) {
		//for(i <- bestFields) {
			println(i+"'s correlation coefficient: "+data.stat.corr(i, fieldName))
		}
	}
	
	def predict(fieldName: String) = {	
		val vectAssembler = new VectorAssembler().setInputCols(goodFields).setOutputCol("features")
		//val vectAssembler = new VectorAssembler().setInputCols(bestFields).setOutputCol("features")
		val withFeatures  = vectAssembler.transform(data.select(
					'HLTHPLN1, 'PERSDOC2,'MEDCOST, 'CHECKUP1, 'EXERANY2, 'SLEPTIM1,
                                	'CVDINFR4, 'CVDCRHD4, 'CVDSTRK3, 'ASTHMA3, 'CHCOCNCR,
                                	'CHCCOPD1, 'HAVARTH3, 'ADDEPEV2, 'CHCKIDNY,  'DIABETE3, 'EMPLOY1,
                                	'CHILDREN, 'INCOME2, 'VETERAN3, '_PHYS14D, '_RFHLTH,
					'GENHLTH,/* 'PHYSHLTH,*/ 'MENTHLTH, 'POORHLTH,
					 data(fieldName)))
		//val withFeatures = vectAssembler.transform(data.select('_RFHLTH, '_PHYS14D, data(fieldName)))
		val linearReg     = new LinearRegression().setLabelCol(fieldName)
		val linearModel   = linearReg.fit(withFeatures)
		for(i <- 0 to goodFields.size-1) {
			println(goodFields(i)+"'s coefficient: "+linearModel.coefficients(i))
		} 
		/*for(i <- 0 to bestFields.size-1) {
                        println(bestFields(i)+"'s coefficient: "+linearModel.coefficients(i))
                }*/
		println("The intercept is: "+linearModel.intercept)
		linearModel.transform(withFeatures).describe("prediction").show()
	}

	val bigFour = Array("GENHLTH", "PHYSHLTH", "MENTHLTH", "POORHLTH") 
	for (j <- bigFour) {
		predict(j)
		correlation(j)
	}



	spark.stop()
}
