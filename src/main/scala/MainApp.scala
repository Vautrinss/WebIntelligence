import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame




object MainApp {
  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("WebIntelligence")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dataFrame: DataFrame = spark.read.json("./data-students.json")
    dataFrame.show
  }
}

