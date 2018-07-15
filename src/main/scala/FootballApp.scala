import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

object FootballApp extends App {

  //implicit val dateEncoder = org.apache.spark.sql.Encoders.kryo[Date]

  val ss = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()

  import ss.implicits._

  val sc = ss.sparkContext

  val footballDf: DataFrame = ss.read
    .option("header", "true")
      .option("dateFormat", "dd/mm/yy")
    .option("inferSchema", "true")
    .csv("E0.csv")
    .toDF("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR", "Referee", "HS", "AS", "HST", "AST", "HF", "AF", "HC", "AC", "HY", "AY", "HR", "AR", "B365H", "B365D", "B365A", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "LBH", "LBD", "LBA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA", "VCH", "VCD", "VCA", "Bb1X2", "BbMxH", "BbAvH", "BbMxD", "BbAvD", "BbMxA", "BbAvA", "BbOU", "BbMx>2.5", "BbAv>2.5", "BbMx<2.5", "BbAv<2.5", "BbAH", "BbAHh", "BbMxAHH", "BbAvAHH", "BbMxAHA", "BbAvAHA", "PSCH", "PSCD", "PSCA")

  val datadf = footballDf.select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").as[FootballData]

  datadf.cache()

  case class FootballData(date: Timestamp, homeTeam: String, awayTeam: String, fthg: Int, ftag: Int, ftr: String)

  import org.apache.spark.sql.functions._

  val data1: DataFrame = datadf.flatMap{ rec =>
    rec.ftr match {
      case "A" => List(rec.awayTeam -> 3)
      case "H" => List(rec.homeTeam -> 3)
      case "D" => List(rec.homeTeam -> 1, rec.awayTeam -> 1)
    }
  }.toDF("teamName","point")

  data1.groupBy("teamName").agg(sum("point")).filter("teamName == 'Chelsea'").show(10)





//  datadf.createOrReplaceTempView("footballdata")
//
//  ss.sql(
//    """
//      | select
//      | CASE FTR
//      |   WHEN "A" THEN AwayTeam
//      |   WHNE "D" THEN
//    """.stripMargin)



  //  sealed trait Ftr {
  //    def value(a: Char)
  //  }
  //
  //  object







}
