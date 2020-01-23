package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation._
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row


object classification extends App {
	val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
	spark.sparkContext.setLogLevel("WARN")	
	
	var admissionData = spark.read.option("delimiter","\t").csv("../../../../data/BigData/admissions/AdmissionAnon.tsv")

//Question: 1
	val admissionRdd = spark.sparkContext.textFile("../../../../data/BigData/admissions/AdmissionAnon.tsv")
	val schemedData = admissionRdd.map { line => line.split("\t")}.map(_.map(x=> 
	  try{x.trim.toDouble} 
	  catch{case e: NumberFormatException => 0.0}
   )).map(x=>Row.fromSeq(x))
   
	val rowSz = schemedData.count()
	val colSz = schemedData.first.size
	println("There are "+rowSz+" rows")  //2946 rows
	println("There are "+colSz+" columns") //47 cols
	
//Question 2:
	val lastCol = admissionData.select('_c46)
	lastCol.distinct.count() //5

//Question 3: 
	admissionData.groupBy('_c46).count().orderBy('_c46).show() //0:642; 1: 15; 2:1068; 3:29; 4:1192
/*
+----+-----+
|_c46|count|
+----+-----+
|   0|  642|
|   1|   15|
|   2| 1068|
|   3|   29|
|   4| 1192|
+----+-----+ */
	
//Question 4: 
//	val schema      = StructType(for(i <- 0 to colSz-1) yield {StructField("c"+i.toString, DoubleType )})
//	val data        = spark.createDataFrame(schemedData, schema).cache()
//	val assembler   = new VectorAssembler().setInputCols((0 to 46).toArray.map(i=>"c"+i.toString)).
//	                  setOutputCol("features")
//                  
//	val assembledDT = assembler.transform(data)
//
//	val Row(coeff1:Matrix) = Correlation.corr(assembledDT, "features").head
//	println(s"Pearson correlation matrix:\n $coeff1")
  /*Pearson correlation matrix:
 1.0                    -0.08515838753061737    ... (47 total)
-0.08515838753061737   1.0                     ...
0.04864301699458359    0.020745978631789064    ...
0.07788055691717045    0.030339382185620118    ...
-0.038256043338390874  -0.021293518795742903   ...
NaN                    NaN                     ...
NaN                    NaN                     ...
NaN                    NaN                     ...
-0.20070844005479277   0.035765348954218325    ...
-0.005310303580434085  -0.009281813515603931   ...
0.2138458271167989     -0.03390513446934602    ...
-0.0663319583048558    0.02248517179414137     ...
-0.024986434524609948  -0.1351385102116024     ...
-0.013489578735152545  -0.11517999067437193    ...
-0.002864834693822084  -0.21448733797201666    ...
-0.00936550080019635   -0.07781493561198714    ...
-0.021153115353745387  -0.07612411805201164    ...
-0.022667761367743712  -0.047806953612249674   ...
-0.01878030281384982   -0.03431407562141609    ...
-0.005981374138126529  0.0539670420002239      ...
0.005732266521715227   0.061688661186114845    ...
0.11162136155543918    -0.06491995874623059    ...
-0.17361678674122746   0.06636868327360329     ...
-0.02147519297825607   -0.026825160929296276   ...
-0.16161681131110445   0.09584379076958775     ...
-0.12248223644003566   -0.020870424595721363   ...
-0.14600858261147293   0.04472490513228729     ...
-0.031070294756292798  0.21091848999131516     ...
-0.13063687754720912   0.016986389798388384    ...
-0.13387557109070738   0.036273444422168934    ...
-0.14262707658823717   0.031701218742845906    ...
-0.11437103957073758   -0.0022440113261264975  ...
-0.12544681296841134   4.926581663905588E-4    ...
-0.04261155285196958   0.02831190248283465     ...
-0.047877062587653355  0.03267727893124143     ...
-0.03715784953044165   0.02382301444515146     ...
-0.04001470108666611   0.03767937467018678     ...
-0.087114747441419     -0.018000158454320427   ...
-0.09729988052092035   -0.008857874726686215   ...
-0.07645819753674062   -0.02709325945922537    ...
0.37926750446561813    -0.07039993999964385    ...
0.5191423641350752     -0.027998319224703288   ...
-0.22999990404550813   -0.1181082983722855     ...
-0.07410180636624072   0.06608035982777133     ...
0.022995059460674778   0.09325971658277293     ...
-0.08801904820580857   -0.01911295581383116    ...
0.06250509305309454    -0.0576096610556067     ...
  */
	
//Question 5: 
//	var matrixWithIdx = Array.fill(47)(Array.fill(47)((0.0, (0,0))))
//	for(i <- 0 to 46){
//	  for(j <- 0 to 46){
//	    matrixWithIdx(i)(j) = (coeff1(i,j), (i, j))
//	  }
//	}
//	matrixWithIdx.flatMap(x=>x).filterNot(x=>x._1.isNaN || x._1==1.0).filter(_._2._2==46).sortBy(x=>math.abs(x._1)).reverse.take(3) 
//	//Array((-0.3885121083933726,(22,46)), (-0.21478089709003176,(23,46)), (-0.09844073158570038,(26,46)))

//Question 6: 
//	val inputVA = new VectorAssembler().setInputCols((0 to 45).toArray.map(i=>"c"+i.toString)).setOutputCol("features")
//	val multiClass = inputVA.transform(data.withColumn("label", floor(col("c46"))))
//	val binaryClass = inputVA.transform(data.withColumn("label", when(col("c46") < 2, 1).otherwise(0)))
//	
//	var Array(trainSet, testSet) =  binaryClass.randomSplit(Array(0.8, 0.2)).map(_.cache())
//	  
//	val rf    = new RandomForestClassifier() 
//	val gbt   = new GBTClassifier()
//	val mlp   = new MultilayerPerceptronClassifier().setLayers(Array(46,2)) 
//	val lscv  = new LinearSVC() 
//	val ovr   = new OneVsRest().setClassifier(gbt) 
//	val naive = new NaiveBayes()
//	
//	val classifiers = Array(rf, gbt, mlp, lscv, ovr, naive)
//	println("For binary classifications: ")
//	for(i <- classifiers) {
//	  val model = i.fit(trainSet)
//	  val predict = model.transform(testSet)
//	  //predict.show()
//	  val binaryEval = new BinaryClassificationEvaluator
//	  val accuracy = binaryEval.evaluate(predict)
//	  println(i.toString+"accuracy = "+ accuracy)
//	} 
//	/* Random forest: 0.8717630955904099
//	 * Gradient Boosted tree: 0.8755733944954113
//	 * Multilayer Perceptron: NaN
//	 * Linear SVC: 0.8562074578277588
//	 * One vs rest:  NaN
//	 * Naive: 0.5039582716780118
//	 */
//	val tup =  multiClass.randomSplit(Array(0.8, 0.2)).map(_.cache())
//	trainSet = tup(0)
//	testSet = tup(1)
//	println("For multiclass classifications: ")
//	for(i <- classifiers) {
//	  val model = i.fit(trainSet)
//	  val predict = model.transform(testSet)
//	  //predict.show()
//	  val multiEval = new MulticlassClassificationEvaluator
//	  val accuracy = multiEval.evaluate(predict)
//	  println("accuracy = "+ accuracy)
//	} 
	/* Random forest: 0.5051162706378124
	 * Gradient Boosted tree: NaN
	 * Multilayer Perceptron: 0.0063
	 * Linear SVC: NaN
	 * One vs rest: 0.5298564451316788
	 * Naive: 0.23840531842222953
	 */
	
} 
