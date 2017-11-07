package utils

import org.apache.spark.sql.SparkSession

object Spark {
  def getSession(): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Bidy")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
