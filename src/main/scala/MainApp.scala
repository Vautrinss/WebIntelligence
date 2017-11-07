import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
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

    case class schemaData2 (
                            appOrSite: String,
                            bidfloor: Double
                          )

    val df: DataFrame = spark.read.json("./data-students.json")
    //val df2 = df.filter("appOrSite is not null")
    val myRDD = df.rdd
    //myRDD.take(20).foreach(println)
    val myDatas2 = myRDD.map{x => x(6)}.take(5)
    myDatas2.foreach(println)

    val myDataEssai = myRDD.map{x => schemaData2(x(0).toString, x(1).asInstanceOf[Double])}
    myDataEssai.take(20).foreach(println)
    //val myDatas = myRDD.map{x =>
      //schemaData(x(0).toString, x(1).toString.toDouble, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString.toBoolean, x(7).toString, x(8).toString, x(9).toString, x(10).toString, x(11).asInstanceOf[], x(12).toString, x(13).toString, x(14).toString)}.toDF()

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

