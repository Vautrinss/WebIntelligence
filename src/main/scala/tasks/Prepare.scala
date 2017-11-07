package tasks

import utils.DSL._
import enumerations.{Column, IndexedColumn, RawColumn}
import org.apache.spark.sql.DataFrame

object Prepare {
  def data(dataFrame: DataFrame): DataFrame = {
    (
      dataFrame
        >> (Fill nullBy(RawColumn.EXCHANGE, "null"))
        >> (Fill nullBy(RawColumn.INTERESTS, "null"))
        >> (Fill nullBy(RawColumn.OS, "null"))
        >> (Fill nullBy(RawColumn.PUBLISHER, "null"))
        >> (Fill nullBy(RawColumn.TYPE, "null"))
        >> (Fill nullBy(RawColumn.BID_FLOOR, 0.0))
        >> (Clean os)
        >> (Clean interests)
        >> (Index strings(
          RawColumn.PUBLISHER,
          RawColumn.TYPE,
          RawColumn.EXCHANGE,
          RawColumn.OS,
          RawColumn.INTERESTS
        ))
        >> (Index booleans RawColumn.LABEL)
        >> (Index bidfloor 2 )
        >> (Convert columnsToVector(Column.FEATURES.toString,
          IndexedColumn.PUBLISHER,
          IndexedColumn.TYPE,
          IndexedColumn.EXCHANGE,
          IndexedColumn.OS,
          IndexedColumn.INTERESTS
        ))

    )
  }
}
