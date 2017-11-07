package models

import enumerations.{Column, ModelType}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame

object Create {

  def model(modelType: ModelType.Value)
           (dataFrameCurried: DataFrame)
           (implicit modelPath: String): DataFrame = {

    implicit val dataFrame: DataFrame = dataFrameCurried

    println(s":: START ${modelType.toString}")

    if (modelPath == "." || modelPath == "/") {
      println(s"/!\\ You can't set the modelPath to `$modelPath` !")
    } else {
      val model = modelType match {
        case ModelType.DECISION_TREE => Some(decisionTreeModel)
        case ModelType.RANDOM_FOREST => Some(randomForestModel)
        case ModelType.LOGISTIC_REGRESSION => Some(logisticRegressionModel)
        case ModelType.GRADIENT_BOOSTED_TREE => Some(withGradientBoostedTre)
        case _ => None
      }
      if (model.isEmpty) {
        println(s"/!\\ You must use a known model !")
      }
    }

    println(s":: END ${modelType.toString}")

    dataFrame
  }

  private def decisionTreeModel(implicit modelPath: String, dataFrame: DataFrame): Unit = {
    val decisionTreeClassifier = new DecisionTreeClassifier()
      .setLabelCol(Column.LABEL.toString)
      .setFeaturesCol(Column.FEATURES.toString)
      .setMaxDepth(30)
      .setMaxBins(19000)

    val classificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(Column.LABEL.toString)
      .setPredictionCol(Column.PREDICTION.toString)
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

  private def randomForestModel(implicit modelPath: String, dataFrame: DataFrame): Unit = {
    val randomForestClassifier = new RandomForestClassifier()
      .setNumTrees(50)
      .setMaxDepth(30)
      .setMaxBins(19000)
      .setLabelCol(Column.LABEL.toString)
      .setFeaturesCol(Column.FEATURES.toString)
      .setPredictionCol(Column.PREDICTION.toString)

    randomForestClassifier
      .fit(dataFrame)
      .write.overwrite.save(modelPath)
  }

  private def logisticRegressionModel(implicit modelPath: String, dataFrame: DataFrame): Unit = {
    val logisticRegression = new LogisticRegression()
      .setLabelCol(Column.LABEL.toString)
      .setFeaturesCol(Column.FEATURES.toString)
      .setPredictionCol(Column.PREDICTION.toString)
      .setRawPredictionCol(s"raw_${Column.PREDICTION.toString}")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setThreshold(0.5)
      .setFamily("auto")

    logisticRegression
      .fit(dataFrame)
      .write.overwrite.save(modelPath)
  }

  private def withGradientBoostedTre(implicit modelPath: String, dataFrame: DataFrame): Unit = {
    /*val gBTClassifier = new GBTClassifier()
      .setLabelCol(Column.LABEL.toString)
      .setFeaturesCol(Column.FEATURES.toString)
      .setPredictionCol(Column.PREDICTION.toString)
      .setRawPredictionCol(s"raw_${Column.PREDICTION.toString}")
      .setMaxIter(10)
      .setMaxDepth(30)
      .setMaxBins(19000)

    gBTClassifier
      .fit(dataFrame)
      .write.overwrite.save(modelPath)
  */}
}
