package tasks

import enumerations.IndexedColumn
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Reduce {
  def labelOccurences(dataFrame: DataFrame): DataFrame = {
    val unfavorizedValues = dataFrame.filter(col(IndexedColumn.LABEL.toString) === 0.0)
    val favorizedValues = dataFrame.filter(col(IndexedColumn.LABEL.toString) === 1.0)
    favorizedValues.union(unfavorizedValues.limit(favorizedValues.count().toInt))
  }
}
