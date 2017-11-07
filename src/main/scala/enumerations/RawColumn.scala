package enumerations

object RawColumn extends Enumeration {
  val APP_OR_SITE: RawColumn.Value = Value("appOrSite")
  val BID_FLOOR: RawColumn.Value = Value("bidfloor")
  val CITY: RawColumn.Value = Value("city")
  val EXCHANGE: RawColumn.Value = Value("exchange")
  val IMPRESSION_ID: RawColumn.Value = Value("impid")
  val INTERESTS: RawColumn.Value = Value("interests")
  val LABEL: RawColumn.Value = Value("label")
  val MEDIA: RawColumn.Value = Value("media")
  val NETWORK: RawColumn.Value = Value("network")
  val OS: RawColumn.Value = Value("os")
  val PUBLISHER: RawColumn.Value = Value("publisher")
  val SIZE: RawColumn.Value = Value("size")
  val TIMESTAMP: RawColumn.Value = Value("timestamp")
  val TYPE: RawColumn.Value = Value("type")
  val USER: RawColumn.Value = Value("user")
}
