package finalProject

import org.apache.spark.ml.regression.LinearRegression
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

import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.styles.HistogramStyle.DataAndColor

object tweetAnalysis extends App {
  
val spark = SparkSession.builder.master("local[*]").getOrCreate()
import spark.implicits._
spark.sparkContext.setLogLevel("WARN")
	
	
val tweetData1 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_1.csv")
val tweetData2 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_2.csv")
val tweetData3 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_3.csv")
val tweetData4 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_4.csv")
val tweetData5 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_5.csv")
val tweetData6 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_6.csv")
val tweetData7 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_7.csv")
val tweetData8 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_8.csv")
val tweetData9 = spark.read.format("csv").option("header", "true").load("IRAhandle_tweets_9.csv")
	
val tweetData  = tweetData1.union(tweetData2).union(tweetData3).union(tweetData4).union(tweetData5).
union(tweetData6).union(tweetData7).union(tweetData8).union(tweetData9)

//tweetData.describe().show()
  
//Basics: 
//Question 1: how many entries are in this data
tweetData.count()   //2973371  

//Question 2: Top language distributions for this data; how many are russians  
val langGroup = tweetData.select('language, 'author).groupBy("language").count.orderBy('count.desc)
langGroup.show()
/*
 * +------------------+-------+                                                    
|          language|  count|
+------------------+-------+
|           English|2108050|
|           Russian| 615233|
|            German|  86403|
|         Ukrainian|  38693|
|           Italian|  17693|
|     United States|  15956|
|           Serbian|   9550|
|             Uzbek|   9401|
|         Bulgarian|   9378|
|LANGUAGE UNDEFINED|   8274|
|            Arabic|   7573|
|        Macedonian|   5202|
|           Unknown|   4654|
|            French|   4469|
|           Spanish|   3113|
|         Norwegian|   2250|
|   Farsi (Persian)|   1684|
|          Romanian|   1630|
|             Dutch|   1163|
|        Azerbaijan|   1059|
+------------------+-------+
 * 
 */
val engLangPer  = 2108050.toDouble/tweetData.count //70.9% 
val russLangPer = 615233.toDouble/tweetData.count // 20.7%

//Question 3: Top region distribution
val regionGroup = tweetData.select('region, 'author).groupBy("region").count.orderBy('count.desc)
regionGroup.show()
/*+--------------------+-------+                                                  
|              region|  count|
+--------------------+-------+
|       United States|2034334|
|             Unknown| 566585|
|          Azerbaijan|  99342|
|United Arab Emirates|  74312|
|  Russian Federation|  37196|
|             Belarus|  29483|
|             Germany|  26912|
|      United Kingdom|  17909|
|               Italy|  12996|
|                Iraq|  10854|
|                null|   8629|
|             Ukraine|   5935|
|            Malaysia|   4886|
|         Afghanistan|   4839|
|              Israel|   3590|
|              France|    925|
|              Canada|    606|
|Iran, Islamic Rep...|    528|
|               Spain|    504|
|               Egypt|    231|
+--------------------+-------+
 */

//Question: date distribution(monthly)
val validMonthData = tweetData.select('publish_date, 'author).filter('publish_date.contains("/") && 
		'publish_date.contains(":")).withColumn("publish_date", split(col("publish_date")," ").getItem(0)).
		withColumn("year", split(col("publish_date"), "/").getItem(2)).withColumn("month", 
		split(col("publish_date"), "/").getItem(0)).select('month, 'year, 'author)

val monthlyActivity = validMonthData.groupBy('year, 'month).count.orderBy('count.desc)
monthlyActivity.show()
/*
+----+-----+------+                                                             
|year|month| count|
+----+-----+------+
|2017|    8|189386|
|2016|   12|153023|
|2016|   10|149435|
|2015|    7|144701|
|2017|    4|133895|
|2017|    1|133801|
|2015|   11|131459|
|2017|    3|126526|
|2016|   11|121276|
|2015|    8|118955|
|2015|   12|116003|
|2016|    9|105518|
|2017|    2| 99227|
|2016|    2| 85080|
|2015|   10| 82226|
|2017|    7| 81740|
|2016|    8| 80702|
|2016|    3| 78929|
|2016|    6| 78121|
|2016|    5| 77176|
+----+-----+------+
*/



// validMonthData.select('publish_date, regexp_replace('publish_date, "/", "-").as("date"))

//validDateData.show()


val corrCols = Array("region", "language", "following", "followers", "updates", "retweet", 
		     "account_type", "account_category")

var corrDF   = tweetData.select('region, 'language, 'following, 'followers, 'updates, 'retweet,
		'account_type, 'account_category).filter(!'region.contains("http") && 
		!'language.contains("http") && !'following.contains("http") && !'following.contains(":")
		&& !'followers.contains(":") && !'followers.contains("http") && 
		('retweet!=1 && 'retweet!=0) && ('account_category.contains("g")|| 'account_category.contains("o") || 'account_category.contains("a") || 'account_category.contains("e")))
val regionTF = udf((row:String)=> if (row!=null && row.trim=="Unknown") 1.0  else 0.0)
val langTF   = udf((row:String)=> if(row!=null && row.trim=="Russian") 1.0  else 0.0)
val rightTF  = udf((row:String)=> if(row!=null && row.trim=="Right") 1.0 else 0.0)
val catTF    = udf((row:String)=> if(row!=null && row=="NonEnglish") 4.0 else if (row!=null && row=="RightTroll") 6.0
		else if(row!=null &&row=="Fearmonger") 5.0 else if(row!=null &&row=="HashtagGamer") 4.0 else if(row!=null &&row=="NewsFeed") 3.0
		else if(row!=null &&row=="Commercial") 2.0 else if(row!=null && row=="LeftTroll") 1 else 0.0)
val numTF    = udf((row:String)=> try{row.toDouble} catch{
		case e:NumberFormatException=>0.0
		case e: NullPointerException=>0.0})
corrDF = corrDF.withColumn("region", regionTF(col("region")).cast(DoubleType))
corrDF = corrDF.withColumn("language", langTF(col("language")).cast(DoubleType))
corrDF = corrDF.withColumn("account_type", rightTF(col("account_type")).cast(DoubleType))
corrDF = corrDF.withColumn("account_category", catTF(col("account_category")).cast(DoubleType))
corrDF = corrDF.withColumn("following", numTF(col("following")).cast(DoubleType))
corrDF = corrDF.withColumn("followers", numTF(col("followers")).cast(DoubleType))
corrDF = corrDF.withColumn("updates", numTF(col("updates")).cast(DoubleType))
corrDF = corrDF.withColumn("retweet", numTF(col("retweet")).cast(DoubleType))


val assembler = new VectorAssembler().setInputCols(corrCols).setOutputCol("features")
val asmbData = assembler.transform(corrDF)
val Row(coeff1:Matrix) = Correlation.corr(asmbData, "features").head()
var matrixWithIdx = Array.fill(8)(Array.fill(8)((0.0, (0,0))))
for(i <- 0 to 7){
	for(j<-0 to 7){
		matrixWithIdx(i)(j) = (coeff1(i, j), (i, j))
	}
}
//matrixWithIdx.flatMap(x=>x).filterNot(x=>x._1==1.0).filter(_._2._2==7).sortBy(x=>math.abs(x._1)).reverse.take(5)
//Array[(Double, (Int, Int))] = Array((0.5589024631151582,(1,7)), (0.4145009932577037,(0,7)), (0.36077594125942186,(6,7)), (-0.12108847902451259,(2,7)), (-0.054799326621410266,(4,7)))
//Array((0.7804926825301549,(6,7)), (0.28461562021831405,(0,7)), (0.07820435485500332,(1,7)), (-0.05507888387485008,(5,7)), (0.03114888914670882,(2,7)))

//Linear regression:
val goodFields = Array("region", "account_type")
var va = new VectorAssembler().setInputCols(goodFields).setOutputCol("features")
val withFeatures = va.transform(corrDF.select('region, 'language, 'account_type, 'account_category))
val linearReg = new LinearRegression().setLabelCol("account_category")
val linearModel = linearReg.fit(withFeatures)
for(i<- 0 to goodFields.size-1){
	println(goodFields(i)+"'s coefficient: "+linearModel.coefficients(i))
}
/*region's coefficient: 0.6963116252820617
account_type's coefficient: 2.865998779166602
*/

println("The interception is: "+linearModel.intercept)
//The interception is: 2.9248982084337265
linearModel.transform(withFeatures).describe("prediction").show()
/*
+-------+------------------+                                                    
|summary|        prediction|
+-------+------------------+
|  count|           2932420|
|   mean|3.7515874260848783|
| stddev| 1.297472227187376|
|    min|2.9248982084337265|
|    max| 6.487208612882391|
+-------+------------------+
*/
//Classification: with language, region and account_type
	val rf    = new RandomForestClassifier() 
	val gbt   = new GBTClassifier()
	val mlp   = new MultilayerPerceptronClassifier().setLayers(Array(3,1)) 
	val lscv  = new LinearSVC() 
	val ovr   = new OneVsRest().setClassifier(gbt) 
	val naive = new NaiveBayes()

	val multiClass = va.transform(corrDF.withColumn("label", col("account_category")))
	val Array(trainSet, testSet) = multiClass.randomSplit(Array(0.8, 0.2)).map(_.cache())
	val model = rf.fit(trainSet)
	val predict = model.transform(testSet) 
	val eval  = new MulticlassClassificationEvaluator
	val accuracy = eval.evaluate(predict) 
	println("Accuracy: "+accuracy)	
	//Random forest: 0.9815427149425662 

	spark.stop()
  
}
