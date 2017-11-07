package tasks

import enumerations.{IndexedColumn, RawColumn}
import org.apache.spark.ml.feature.{QuantileDiscretizer, StringIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, udf}

object Index {

  def strings(columnNames: RawColumn.Value*)(dataFrame: DataFrame): DataFrame = {
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

  def booleans(columnNames: RawColumn.Value*)(dataFrame: DataFrame): DataFrame = {
    val replacer = udf((value: Boolean) => if (value) 1.0 else 0.0)
    def indexer(columnName: String, dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn(s"${columnName}_indexed", replacer(dataFrame(columnName)))
    }
    columnNames.foldLeft(dataFrame) { (previousDataFrame, columnName) => {
      indexer(columnName.toString, previousDataFrame)
    }}
  }

  def bidfloor(bucketsNumber: Int)(dataFrame: DataFrame): DataFrame = {
    val discretizer = new QuantileDiscretizer()
      .setInputCol(RawColumn.BID_FLOOR.toString)
      .setOutputCol(IndexedColumn.BID_FLOOR.toString)
      .setNumBuckets(bucketsNumber)

    val result = discretizer.fit(dataFrame).transform(dataFrame)

    result
  }

  def arraysOfLongs(columnNames: RawColumn.Value*)(dataFrame: DataFrame): DataFrame = {
    def indexer(columnName: String, dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn(s"${columnName}_as_string", concat_ws(" ", col(columnName)))
    }
    val dfTemp = columnNames.foldLeft(dataFrame) { (previousDataFrame, columnName) => {
      indexer(columnName.toString, previousDataFrame)
    }}

    val indexers = columnNames.map { columnName => {
      new StringIndexer()
        .setHandleInvalid("keep")
        .setInputCol(s"${columnName.toString}_as_string")
        .setOutputCol(s"${columnName.toString}_indexed")
        .fit(dfTemp)
    }}
    indexers.foldLeft(dfTemp) { (df, indexer) => {
      indexer.transform(df)
    }}
  }

}
