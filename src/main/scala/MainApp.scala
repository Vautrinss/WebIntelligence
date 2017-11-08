import org.apache.spark.ml.feature.{IndexToString, QuantileDiscretizer, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions.{col, concat_ws, udf}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import scala.collection.immutable.HashMap


object MainApp {


  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("WebIntelligence")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    case class schemaData(
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




    val rightName = HashMap(
      "android" -> "ANDROID",
      "iOS" -> "IOS",
      "Windows Phone OS" -> "WINDOWS",
      "other" -> "UNKNOWN",
      "Unknown" -> "UNKNOWN",
      "blackberry" -> "BLACKBERRY",
      "WebOS" -> "WEBOS",
      "WindowsPhone" -> "WINDOWS",
      "Windows Mobile OS" -> "WINDOWS",
      "WindowsMobile" -> "WINDOWS",
      "Android" -> "ANDROID",
      "Symbian" -> "SYMBIAN",
      "Rim" -> "BLACKBERRY",
      "ios" -> "IOS",
      "Bada" -> "BADA",
      "null" -> "NULL",
      "windows" -> "WINDOWS"
    )

    var df: DataFrame = spark.read.json("./data-students.json")

    df = df.na.fill("null", Seq("exchange"))
    df = df.na.fill("null", Seq("interests"))
    df = df.na.fill("null", Seq("os"))
    df = df.na.fill("null", Seq("publisher"))
    df = df.na.fill("null", Seq("type"))
    df = df.na.fill(0.0, Seq("bidfloor"))


    val ajuster = udf((col: String) => rightName(col).toString)
    df = df.withColumn("os", ajuster(df("os")))

    val ajuster2 = udf((col: String) => {
      val values = col.split(",")
      var result = "null"
      if (values.nonEmpty) {
        var i = 0
        while (i < values.length && result == "null") {
          if (!values(i).startsWith("IAB")) result = values(i)
            i = i + 1
        }
        result
      }
      else "null"
    })
    df = df.withColumn("interests", ajuster2(df("interests")))

    //-------------INDEX--------------
    df = indexTypeString(df, "publisher", "type", "exchange", "os", "interests")
    df = indexTypeBool(df, "label")
    df = indexTypeBidfloor(df, 2)

    //-------------CONVERT------------"publisherIndexed"
    df = columnsToVector(df, "features", "typeIndexed", "exchangeIndexed", "osIndexed", "interestsIndexed")

    df.show()

    randomForestModelLeo(df)

    def indexTypeString(dataFrame: DataFrame, columnNames: String*): DataFrame = {
      val indexers = columnNames.map { columnName => {
        new StringIndexer()
          .setInputCol(columnName)
          .setOutputCol(s"${columnName}Indexed")
          .fit(dataFrame)
      }
      }
      indexers.foldLeft(dataFrame) { (df, indexer) => {
        indexer.transform(df)
      }
      }
    }

    def indexTypeBool(dataFrame: DataFrame, columnNames: String*): DataFrame = {
      val replacer = udf((value: Boolean) => if (value) 1.0 else 0.0)

      def indexer(columnName: String, dataFrame: DataFrame): DataFrame = {
        dataFrame.withColumn(s"${columnName}Indexed", replacer(dataFrame(columnName)))
      }

      columnNames.foldLeft(dataFrame) { (previousDataFrame, columnName) => {
        indexer(columnName.toString, previousDataFrame)
      }
      }
    }

    def indexTypeBidfloor(dataFrame: DataFrame, bucketsNumber: Int): DataFrame = {
      val discretizer = new QuantileDiscretizer()
        .setInputCol("bidfloor")
        .setOutputCol("bidfloorIndexed")
        .setNumBuckets(bucketsNumber)

      val result = discretizer.fit(dataFrame).transform(dataFrame)

      result
    }

    def columnsToVector(dataFrame: DataFrame, vectorName: String, columnNames: String*): DataFrame = {
      new VectorAssembler()
        .setInputCols(columnNames.map {
          _.toString
        }.toArray)
        .setOutputCol(vectorName)
        .transform(dataFrame)
    }

   //------------GX-----------------

    def randomForestModel(implicit modelPath: String, dataFrame: DataFrame): Unit = {
      val randomForestClassifier = new RandomForestClassifier()
        .setNumTrees(10)
        //.setMaxDepth(30)
        //.setMaxBins(19000)
        .setLabelCol("labelIndexed")
        .setFeaturesCol("features")
        //.setPredictionCol("prediction")

      randomForestClassifier
        .fit(dataFrame)
        .write.overwrite.save(modelPath)


    }

    def decisionTreeModel(implicit modelPath: String, dataFrame: DataFrame): Unit = {
      val decisionTreeClassifier = new DecisionTreeClassifier()
        .setLabelCol("labelIndexed")
        .setFeaturesCol("features")
        .setMaxDepth(30)
        .setMaxBins(19000)

      val classificationEvaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("labelIndexed")
        .setPredictionCol("prediction")
        .setMetricName("weightedRecall")

      val paramGrid = new ParamGridBuilder()
        .build()

      val crossValidator = new CrossValidator()
        .setNumFolds(5)
        .setEstimator(decisionTreeClassifier)
        .setEvaluator(classificationEvaluator)
        .setEstimatorParamMaps(paramGrid)

      crossValidator
        .fit(dataFrame)
        .bestModel
        .asInstanceOf[DecisionTreeClassificationModel]
        .write.overwrite.save(modelPath)
    }


    //------------LEO---------------

    def randomForestModelLeo(df: DataFrame) = {
      val labelIndexer = new StringIndexer()
        .setInputCol("labelIndexed")
        .setOutputCol("indexedLabel")
        .fit(df)
      // Automatically identify categorical features, and index them.
      // Set maxCategories so features with > 4 distinct values are treated as continuous.
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(df)

      // Split the data into training and test sets (30% held out for testing).
      val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

      // Train a RandomForest model.
      val rf = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setNumTrees(10)

      // Convert indexed labels back to original labels.
      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      // Chain indexers and forest in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

      // Train model. This also runs the indexers.
      val model = pipeline.fit(trainingData)

      // Make predictions.
      val predictions = model.transform(testData)

      // Select example rows to display.
      predictions.select("predictedLabel", "label", "features").show(5)

      // Select (prediction, true label) and compute test error.
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println("Test Error = " + (1.0 - accuracy))

      val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
      println("Learned classification forest model:\n" + rfModel.toDebugString)
    }

    def gradientBoostedLeo(df : DataFrame) = {
      // Index labels, adding metadata to the label column.
      // Fit on whole dataset to include all labels in index.
      val labelIndexer = new StringIndexer()
        .setInputCol("labelIndexed")
        .setOutputCol("indexedLabel")
        .fit(df)
      // Automatically identify categorical features, and index them.
      // Set maxCategories so features with > 4 distinct values are treated as continuous.
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(df)

      // Split the data into training and test sets (30% held out for testing).
      val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

      // Train a GBT model.
      val gbt = new GBTClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setMaxIter(10)

      // Convert indexed labels back to original labels.
      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      // Chain indexers and GBT in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

      // Train model. This also runs the indexers.
      val model = pipeline.fit(trainingData)

      // Make predictions.
      val predictions = model.transform(testData)

      // Select example rows to display.
      predictions.select("predictedLabel", "label", "features").show(5)

      // Select (prediction, true label) and compute test error.
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println("Test Error = " + (1.0 - accuracy))

      val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
      println("Learned classification GBT model:\n" + gbtModel.toDebugString)
    }
  }
}