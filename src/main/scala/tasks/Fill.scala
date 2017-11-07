package tasks

import enumerations.RawColumn
import org.apache.spark.sql.DataFrame

object Fill {
  def nullBy(columnName: RawColumn.Value, newValue: String)(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.fill(newValue, Seq(columnName.toString))
  }

  def nullBy(columnName: RawColumn.Value, newValue: Double)(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.fill(newValue, Seq(columnName.toString))
  }
}
