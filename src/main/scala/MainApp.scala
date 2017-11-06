import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions.desc




object MainApp {
  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("WebIntelligence")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df: DataFrame = spark.read.json("./data-students.json")
    df.groupBy("label", "appOrSite").count().orderBy(desc("count")).show(500, false)

  }
}

