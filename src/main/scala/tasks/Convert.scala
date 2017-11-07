package tasks

import enumerations.{IndexedColumn, RawColumn}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

object Convert {
  def columnsToVector(vectorName: String, columnNames: IndexedColumn.Value*)(dataFrame: DataFrame): DataFrame = {
    new VectorAssembler()
      .setInputCols(columnNames.map {_.toString }.toArray)
      .setOutputCol(vectorName)
      .transform(dataFrame)
  }

  def doublesToBooleans(columnNames: RawColumn.Value*)(dataFrame: DataFrame): DataFrame = {
    val replacer = udf((value: Double) => value == 1.0)
    def indexer(columnName: String, dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn(columnName, replacer(dataFrame(columnName)))
    }
    columnNames.foldLeft(dataFrame) { (previousDataFrame, columnName) => {
      indexer(columnName.toString, previousDataFrame)
    }}
  }
}
