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

    val spark: SparkSession = SparkSession
      .builder()
      .appName("WebIntelligence")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    case class schemaData (
                            appOrSite: String,
                            bidfloor: Double,
                            city: String,
                            exchange: String,
                            impid: String,
                            interests: String,
                            label: String,
                            media: String,
                            network: String,
                            os: String,
                            publisher: String,
                            typeI: String,
                            user: String
                          )

    case class schemaData2 (
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
                            timestamp: Long
                           )

    val df: DataFrame = spark.read.json("./data-students.json")
    //val df2 = df.filter("appOrSite is not null")
    df.drop("size")
    df.drop("timestamp")
    val mm = df.select($"label" cast "string").as[schemaData]

    mm.show(10)
    val myRDD = df.rdd
    //myRDD.take(20).foreach(println)
    //val myDatas2 = df.map{x => x(6)}.take(5)
    //myDatas2.foreach(println)
    //df.where(df("city").isNull).groupBy("label", "city").count().orderBy(desc("count")).show(500, false)

    /*val myDataEssai = myRDD
      .filter(t => t(2) != null)
      .filter(t => t(3) != null)
      .filter(t => t(4) != null)
      .filter(t => t(11) != null)

      .map{x => schemaData2(x(0).toString, x(1).asInstanceOf[Double], x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).asInstanceOf[Boolean], x(7).toString, x(8).toString, x(9).toString, x(10).toString, x(11).asInstanceOf[Long])}
    myDataEssai.take(5).foreach(println)*/
/*
    val myDatas = spark.read.json("./data-students.json").drop("size").drop("timestamp")
      .filter(t => t(2) != null)
      .filter(t => t(3) != null)
      .filter(t => t(4) != null)
      .filter(t => t(5) != null)
      .filter(t => t(7) != null)
      .filter(t => t(8) != null)
      .filter(t => t(9) != null)
      .filter(t => t(10) != null)
      .filter(t => t(12) != null)
      .map(x =>
      schemaData(x(0).toString, x(1).asInstanceOf[Double], x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString, x(7).toString, x(8).toString, x(9).toString, x(10).toString, x(11).toString, x(12).toString)).toDF
    myDatas.take(5).foreach(println)

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
    val lrModel = lrPipeline.fit(myDatas)
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

