import config._
import org.apache.spark.sql.SparkSession

object WordCountApp extends App{

  val ss = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()

  val sc = ss.sparkContext

  //Rdd Operations

  val textFileRdd= sc.textFile("testfile.txt")

  val wordCounts= textFileRdd.flatMap(line => line.split(" ")).map(s=> (s,1)).reduceByKey((a,b) => a+b)
  wordCounts.foreach(println)

  import ss.implicits._

  val x = (1 to 10).toList
  val numbersDf = x.toDF

  println(s"Rdd partitions:::: ${numbersDf.rdd.getNumPartitions}")


}
