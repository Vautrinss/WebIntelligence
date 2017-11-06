import org.apache.spark.ml.feature.StringIndexer

class DataSchema (
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
{
/*
  def inputsIndexer(col: String, outCol: String) = {
    return new StringIndexer().setInputCol(col).setOutputCol(col + "Indexer")
  }
  */
}
