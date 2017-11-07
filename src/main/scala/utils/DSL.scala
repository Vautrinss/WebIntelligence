package utils

import org.apache.spark.sql.DataFrame

object DSL {

  implicit class DataFrameDSL(dataFrame: DataFrame) {
    def >>(f: (DataFrame) => DataFrame): DataFrame = {
      f(dataFrame)
    }
  }

}