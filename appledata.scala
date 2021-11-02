
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

val apple = sc.textFile("/stocks/apple_daily.csv")
apple.collect.foreach(println)
apple.first()
case class appClass(date:String, open:Double, high:Double, low:Double, close:Double, volume:Long, adjClose:Double)
val header = apple.first()
val appleT = apple.filter(row => row != header)
val appleData = appleT.map(x=>x.split(",")).map(x=>appClass(x(0).toString,x(1).toDouble,x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toLong,x(6).toDouble))
appleData.collect.foreach(println)

val appleDF=appleData.toDF()
appleDF.show()
appleDF.printSchema
val appleRDD=appleDF.rdd
appleRDD.collect.foreach(println)



val tdate = to_timestamp($"date", "yyyy-MM-dd")


val anewapple = appleDF.withColumn("DateIs", tdate)

anewapple.createOrReplaceTempView("appledata")

spark.sql("select  year(DateIs) as Year, count(year(DateIs)) as Counter from appledata group by year(DateIs) sort by year(DateIs)").show()
