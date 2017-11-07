package enumerations

object IndexedColumn extends Enumeration {
  val APP_OR_SITE: IndexedColumn.Value = Value("appOrSite_indexed")
  val BID_FLOOR: IndexedColumn.Value = Value("bidfloor_indexed")
  val CITY: IndexedColumn.Value = Value("city_indexed")
  val EXCHANGE: IndexedColumn.Value = Value("exchange_indexed")
  val IMPRESSION_ID: IndexedColumn.Value = Value("impid_indexed")
  val INTERESTS: IndexedColumn.Value = Value("interests_indexed")
  val LABEL: IndexedColumn.Value = Value("label_indexed")
  val MEDIA: IndexedColumn.Value = Value("media_indexed")
  val NETWORK: IndexedColumn.Value = Value("network_indexed")
  val OS: IndexedColumn.Value = Value("os_indexed")
  val PUBLISHER: IndexedColumn.Value = Value("publisher_indexed")
  val SIZE: IndexedColumn.Value = Value("size_indexed")
  val TIMESTAMP: IndexedColumn.Value = Value("timestamp_indexed")
  val TYPE: IndexedColumn.Value = Value("type_indexed")
  val USER: IndexedColumn.Value = Value("user_indexed")
}
