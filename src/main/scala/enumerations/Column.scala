package enumerations

object Column extends Enumeration {
  val LABEL: Column.Value = Value(IndexedColumn.LABEL.toString)
  val FEATURES: Column.Value = Value("features")
  val PREDICTION: Column.Value = Value("prediction")
}
