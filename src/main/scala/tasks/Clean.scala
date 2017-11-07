package tasks

import enumerations.{OS, RawColumn}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import scala.collection.immutable.HashMap

object Clean {
  def os(dataFrame: DataFrame): DataFrame = {
    val normalizer = HashMap(
      "android"->OS.ANDROID,
      "iOS"->OS.IOS,
      "Windows Phone OS"->OS.WINDOWS,
      "other"->OS.UNKNOWN,
      "Unknown"->OS.UNKNOWN,
      "blackberry"->OS.BLACKBERRY,
      "WebOS"->OS.WEBOS,
      "WindowsPhone"->OS.WINDOWS,
      "Windows Mobile OS"->OS.WINDOWS,
      "WindowsMobile"->OS.WINDOWS,
      "Android"->OS.ANDROID,
      "Symbian"->OS.SYMBIAN,
      "Rim"->OS.BLACKBERRY,
      "ios"->OS.IOS,
      "Bada"->OS.BADA,
      "null"->OS.NULL,
      "windows"->OS.WINDOWS
    )
    val replacer = udf((col: String) => normalizer(col).toString)
    dataFrame.withColumn(RawColumn.OS.toString, replacer(dataFrame(RawColumn.OS.toString)))
  }

  def interests(dataFrame: DataFrame): DataFrame = {
    val replacer = udf((col: String) => {
      val values = col.split(",")
      if (values.nonEmpty) values(0) else "null"
    })
    dataFrame.withColumn(RawColumn.INTERESTS.toString, replacer(dataFrame(RawColumn.INTERESTS.toString)))
  }
}
