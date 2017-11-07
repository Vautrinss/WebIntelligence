package models

import enumerations.{Column, ModelType, RawColumn}
import org.apache.spark.ml.classification._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import tasks.Convert
import utils.Show

object Use {

  def model(modelType: ModelType.Value, modelPath: String)
           (dataFrame: DataFrame): DataFrame = {

    println(s":: START ${modelType.toString}")

    val model = modelType match {
      case ModelType.DECISION_TREE =>
        Some(DecisionTreeClassificationModel.read.load(modelPath))
      case ModelType.RANDOM_FOREST =>
        Some(RandomForestClassificationModel.read.load(modelPath))
      case ModelType.LOGISTIC_REGRESSION =>
        Some(LogisticRegressionModel.read.load(modelPath))
      case ModelType.GRADIENT_BOOSTED_TREE =>
        Some(GBTClassificationModel.read.load(modelPath))
      case _ =>
        None
    }

    if (model.isDefined) {
      val predictions = model.get.transform(dataFrame)
      Show.metrics(Column.LABEL.toString, Column.PREDICTION.toString, predictions)
      println(s":: END ${modelType.toString}")

      // Clean output
      predictions
        .select(
          (Column.PREDICTION.toString // Take in first position the Prediction column name
            :: RawColumn.values.map(value => value.toString).toList) // Add just initial columns names
            .map((column: String) => col(column)): _* // Convert all names into real columns
        )
        .drop(col(RawColumn.SIZE.toString)) // Remove size because CSV data source does not support array<bigint> data type
        .drop(col(RawColumn.LABEL.toString)) // Remove initial Label column
        .withColumnRenamed(Column.PREDICTION.toString, RawColumn.LABEL.toString) // Rename Prediction column into Label column
        .transform(Convert.doublesToBooleans(RawColumn.LABEL))
    } else {
      println(s"/!\\ Can't load the model !")
      dataFrame
    }
  }

}
