import org.apache.spark.sql.SparkSession

package object config {

    val getSparkContext = getSparkSession.sparkContext

    val getSparkSession = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()


}
