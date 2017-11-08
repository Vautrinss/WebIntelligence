import enumerations.{Column, IndexedColumn, OS, RawColumn}
import org.apache.spark.ml.feature.{QuantileDiscretizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import tasks._
import org.apache.spark.sql.functions.{col, concat_ws, udf}

import scala.collection.immutable.HashMap


object MainApp {


  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("WebIntelligence")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    case class schemaData (
                            appOrSite: String,
                            bidfloor: Double,
                            city: String,
                            exchange: String,
                            impid: String,
                            interests: String,
                            label: Boolean,
                            media: String,
                            network: String,
                            os: String,
                            publisher: String,
                            size: Array[Long],
                            timestamp: Long,
                            typeI: String,
                            user: String
                          )




    val rightName = HashMap(
      "android"->"ANDROID",
      "iOS"->"IOS",
      "Windows Phone OS"->"WINDOWS",
      "other"->"UNKNOWN",
      "Unknown"->"UNKNOWN",
      "blackberry"->"BLACKBERRY",
      "WebOS"->"WEBOS",
      "WindowsPhone"->"WINDOWS",
      "Windows Mobile OS"->"WINDOWS",
      "WindowsMobile"->"WINDOWS",
      "Android"->"ANDROID",
      "Symbian"->"SYMBIAN",
      "Rim"->"BLACKBERRY",
      "ios"->"IOS",
      "Bada"->"BADA",
      "null"->"NULL",
      "windows"->"WINDOWS"
    )

    var df: DataFrame = spark.read.json("./data-students.json")

    df = df.na.fill("null", Seq(RawColumn.EXCHANGE.toString))
    df = df.na.fill("null", Seq(RawColumn.INTERESTS.toString))
    df = df.na.fill("null", Seq(RawColumn.OS.toString))
    df = df.na.fill("null", Seq(RawColumn.PUBLISHER.toString))
    df = df.na.fill("null", Seq(RawColumn.TYPE.toString))
    df = df.na.fill(0.0, Seq(RawColumn.BID_FLOOR.toString))


    val ajuster = udf((col: String) => rightName(col).toString)
    df = df.withColumn(RawColumn.OS.toString, ajuster(df(RawColumn.OS.toString)))


    val ajuster2 = udf((col: String) => {
      val values = col.split(",")
      if (values.nonEmpty) values(0) else "null"
    })
    df = df.withColumn(RawColumn.INTERESTS.toString, ajuster2(df(RawColumn.INTERESTS.toString)))

    //-------------INDEX--------------
    df = typeString(df, RawColumn.PUBLISHER, RawColumn.TYPE, RawColumn.EXCHANGE, RawColumn.OS, RawColumn.INTERESTS)
    df = typeBool(df, RawColumn.LABEL)
    df = typeBidfloor(df, 2)

    //-------------CONVERT------------
    df = columnsToVector(df, Column.FEATURES.toString, IndexedColumn.PUBLISHER, IndexedColumn.TYPE, IndexedColumn.EXCHANGE, IndexedColumn.OS, IndexedColumn.INTERESTS)
    df.show()

    /*
          val df3 = Reduce labelOccurences df2
          //val df4 = Create model(ModelType.DECISION_TREE, df3)
          df3.show()*/

    def typeString(dataFrame: DataFrame, columnNames: RawColumn.Value*): DataFrame = {
      val indexers = columnNames.map { columnName => {
        new StringIndexer()
          .setInputCol(columnName.toString)
          .setOutputCol(s"${columnName.toString}_indexed")
          .fit(dataFrame)
      }}
      indexers.foldLeft(dataFrame) { (df, indexer) => {
        indexer.transform(df)
      }}
    }

    def typeBool(dataFrame: DataFrame, columnNames: RawColumn.Value*): DataFrame = {
      val replacer = udf((value: Boolean) => if (value) 1.0 else 0.0)
      def indexer(columnName: String, dataFrame: DataFrame): DataFrame = {
        dataFrame.withColumn(s"${columnName}_indexed", replacer(dataFrame(columnName)))
      }
      columnNames.foldLeft(dataFrame) { (previousDataFrame, columnName) => {
        indexer(columnName.toString, previousDataFrame)
      }}
    }

    def typeBidfloor(dataFrame: DataFrame, bucketsNumber: Int): DataFrame = {
      val discretizer = new QuantileDiscretizer()
        .setInputCol(RawColumn.BID_FLOOR.toString)
        .setOutputCol(IndexedColumn.BID_FLOOR.toString)
        .setNumBuckets(bucketsNumber)

      val result = discretizer.fit(dataFrame).transform(dataFrame)

      result
    }

    def columnsToVector(dataFrame: DataFrame, vectorName: String, columnNames: IndexedColumn.Value*): DataFrame = {
      new VectorAssembler()
        .setInputCols(columnNames.map {_.toString }.toArray)
        .setOutputCol(vectorName)
        .transform(dataFrame)
    }
  }
}