import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameApp {

  def main(args: Array[String]): Unit = {

    //creates a DataFrame based on the content of a JSON file

    val ss = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()

    val empDF: DataFrame = ss.read
      .option("multiline", true).option("mode", "PERMISSIVE")
      .json("employee.json")

    empDF.show()

    val empCsvDF: DataFrame = ss.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("employee.csv")
      .toDF("name","rollno","address")

    empCsvDF.printSchema()

    empCsvDF.show()


    empDF.select("name").show()

    import ss.implicits._

    empDF.select($"name", $"rollno"+ 1).show()

    empDF.filter($"rollno" > 788).show()

    empDF.groupBy("name").count().show()

    empDF.createOrReplaceTempView("employee")

    ss.sql("Select * from employee").show()

    empDF.createGlobalTempView("employeeG")

    ss.sql("Select * from global_temp.employeeG").show()

    ss.newSession().sql("SELECT * FROM global_temp.employeeG").show()



  }



}
