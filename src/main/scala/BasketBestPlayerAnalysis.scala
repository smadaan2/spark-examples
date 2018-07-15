import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object BasketBestPlayerAnalysis extends App {

  val ss = SparkSession.builder().appName("BasketPlayer").master("local[*]").getOrCreate()
  val sc = ss.sparkContext

  import ss.implicits._

  val basketDf = ss.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("basket.csv")

  basketDf.printSchema()

  val partionDf = basketDf.repartition(sc.defaultParallelism).as[BasketData]
  
  case class BasketData(rk:Int,player:String)

  //filter out junk rows, clean up data entry errors as well
  //val filteredStats = partionDf.filter(row=>row.getAs[String](1) != null )
  //println(filteredStats.take(2))

  //filteredStats.cache()







}
