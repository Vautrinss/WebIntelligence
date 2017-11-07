import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import utils.{Data, Spark}
import tasks._
import models.Create
import enumerations.ModelType
import utils.DSL._
import org.apache.spark.sql.functions.udf
import scala.collection.immutable.HashMap

import org.apache.spark.SparkContext._
import org.apache.spark.sql._


import org.apache.spark.sql.functions.desc

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StandardScaler

import org.apache.spark.sql.types._


object MainApp {


  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("WebIntelligence")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    case class schemaData (
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

    object OS extends Enumeration {
      val ANDROID, IOS, WINDOWS, UNKNOWN, BLACKBERRY, WEBOS, SYMBIAN, BADA, NULL = Value
    }

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

    val df: DataFrame = spark.read.json("./data-students.json")

      val df2 = Prepare data df
      val df3 = Reduce labelOccurences df2
      //val df4 = Create model(ModelType.DECISION_TREE, df3)
      df3.show()

    /*df.show(50)
    def interests(dataFrame: DataFrame): DataFrame = {
      val replacer = udf((col: String) => {
        val values = col.split(",")
        if (values.nonEmpty) values(0) else "null"
      })
      dataFrame.withColumn("interests", replacer(dataFrame("interests")))
    }

    df.na.fill("NO", Seq("exchange"))
    val df2 = df.na.fill("NO", Seq("interests"))
    df.na.fill("NO", Seq("os"))
    df.na.fill("NO", Seq("publisher"))
    df.na.fill("NO", Seq("type"))
    df.na.fill(0.0, Seq("bidfloor"))

    val normalizer = HashMap(
      "android"->OS.ANDROID,
      "iOS"->OS.IOS,
      "Windows Phone OS"->OS.WINDOWS,
      "other"->OS.UNKNOWN,
      "Unknown"->OS.UNKNOWN,
      "blackberry"->OS.BLACKBERRY,
      "WebOS"->OS.WEBOS,
      "WindowsPhone"->OS.WINDOWS,
      "Windows Mobile OS"->OS.WINDOWS,
      "WindowsMobile"->OS.WINDOWS,
      "AndroidP"->OS.ANDROID,
      "Symbian"->OS.SYMBIAN,
      "Rim"->OS.BLACKBERRY,
      "ios"->OS.IOS,
      "Bada"->OS.BADA,
      "null"->OS.NULL,
      "windows"->OS.WINDOWS
    )
    val x = udf((col: String) => normalizer(col).toString)
    df.withColumn("os", x(df("os")))

    val df3 = interests(df2)



    df3.show(50)*/

    /*
        /* Here we have to clean datas with .filter() */
        // Maybe try to convert label to Int or to String
        //tranformor to convert string to category values
        val appOrSiteIndexer = new StringIndexer().setInputCol("appOrSite").setOutputCol("appOrSiteCat")
        val bidfloorIndexer = new StringIndexer().setInputCol("bidfloor").setOutputCol("bidfloorCat")
        val cityIndexer = new StringIndexer().setInputCol("city").setOutputCol("cityCat")
        val exchangeIndexer = new StringIndexer().setInputCol("exchange").setOutputCol("exchangeCat")
        val impidIndexer = new StringIndexer().setInputCol("impid").setOutputCol("impidCat")
        val interestsIndexer = new StringIndexer().setInputCol("interests").setOutputCol("interestsCat")
        val mediaIndexer = new StringIndexer().setInputCol("media").setOutputCol("mediaCat")
        val networkIndexer = new StringIndexer().setInputCol("network").setOutputCol("networkCat")
        val osIndexer = new StringIndexer().setInputCol("os").setOutputCol("osCat")
        val publisherIndexer = new StringIndexer().setInputCol("publisher").setOutputCol("publisherCat")
        val typeIndexer = new StringIndexer().setInputCol("type").setOutputCol("typeCat")
        val userIndexer = new StringIndexer().setInputCol("user").setOutputCol("userCat")
        //val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndexer")
        //val df4 = appOrSiteIndexer.transform(df)
        //df4.show()
        val assembler = new VectorAssembler().setInputCols(Array(
          "appOrSiteCat",
          "bidfloorCat",
          "cityCat",
          "exchangeCat",
          "impidCat",
          "interestsCat",
          "mediaCat",
          "networkCat",
          "osCat",
          "publisherCat",
          "typeCat",
          "userCat"
        )).setOutputCol("rawFeatures")
        //vestor slicer
        val slicer = new VectorSlicer().setInputCol("rawFeatures").setOutputCol("slicedfeatures").setNames(Array(
          "appOrSiteCat",
          "bidfloorCat",
          "cityCat",
          "exchangeCat",
          "impidCat",
          "interestsCat",
          "mediaCat",
          "networkCat",
          "osCat",
          "publisherCat",
          "typeCat",
          "userCat"))
        //scale the features
        val scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("features").setWithStd(true).setWithMean(true)
        //labels for binary classifier
        val binarizerClassifier = new Binarizer().setInputCol("label").setOutputCol("binaryLabel").setThreshold(15.0)
        //logistic regression
        val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("binaryLabel").setFeaturesCol("features")
        // Chain indexers and tree in a Pipeline
        val lrPipeline = new Pipeline().setStages(Array(
          appOrSiteIndexer,
          bidfloorIndexer,
          cityIndexer,
          exchangeIndexer,
          impidIndexer,
          interestsIndexer,
          mediaIndexer,
          networkIndexer,
          osIndexer,
          publisherIndexer,
          typeIndexer,
          userIndexer,
          assembler, slicer, scaler, binarizerClassifier, lr))
        // Train model.
        val lrModel = lrPipeline.fit(df)
        // Make predictions.
        val lrPredictions = lrModel.transform(df)
        // Select example rows to display.
        lrPredictions.select("prediction", "binaryLabel", "features").show(20)
        //val df3 = assembler.transform(df)
        //df3.show()
        //df.groupBy("label", "appOrSite").count().orderBy(desc("count")).show()
    */
  }
}