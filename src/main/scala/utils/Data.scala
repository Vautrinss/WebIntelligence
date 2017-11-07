package utils

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.spark.sql.{DataFrame, SparkSession}

object Data {
  def read(filePath: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.json(filePath)
  }

  def write(filePath: String)(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .coalesce(1) // Merge all partitions to write just one csv file
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .save("tmp")

    // Move the real csv file inside `tmp` to the app's root
    val tmpDirectory = new File("tmp")
    if (tmpDirectory.exists && tmpDirectory.isDirectory) {
      val tmpFiles = tmpDirectory.listFiles.filter(_.isFile).toList.filter(_.getName.endsWith(".csv"))
      if (tmpFiles.nonEmpty) {
        val csvTmpPath = tmpFiles.head.toPath
        val finalPath = new File(filePath).toPath
        Files.move(csvTmpPath, finalPath, StandardCopyOption.REPLACE_EXISTING)
      }
    }

    dataFrame
  }
}
