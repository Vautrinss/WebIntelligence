package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, not}

object Show {
  def metrics(labelColumn: String, predictionColumn: String, predictions: DataFrame): Unit = {
    println(":: => calculate metrics about predictions")

    val labelPrediction = predictions.select(labelColumn, predictionColumn)

    val totalPredictions = predictions.count()
    println(s":: () Total: \u0009\u0009\u0009 $totalPredictions")

    val correct = labelPrediction
      .filter(col(labelColumn) === col(predictionColumn))
      .count()

    val ratioCorrect = correct.toDouble / totalPredictions.toDouble
    println(s":: () Correct: \u0009\u0009\u0009 $correct ($ratioCorrect)")

    val wrong = labelPrediction
      .filter(not(col(labelColumn) === col(predictionColumn)))
      .count()

    val ratioWrong = wrong.toDouble / totalPredictions.toDouble
    println(s":: () Wrong: \u0009\u0009\u0009 $wrong ($ratioWrong)")

    val clickedPredicted = labelPrediction
      .filter(col(predictionColumn) === 1.0)
      .count()
    println(s":: () Clicked predicted: $clickedPredicted (NOT: ${totalPredictions-clickedPredicted})")

    val clickedReal = labelPrediction
      .filter(col(labelColumn) === 1.0)
      .count()
    println(s":: () Clicked real: \u0009 $clickedReal (NOT: ${totalPredictions-clickedReal})")

    println()

    val truePositive = labelPrediction
      .filter(col(predictionColumn) === 1.0) // Predicted clicked
      .filter(col(labelColumn) === 1.0) // Actually clicked
      .count()
    println(s":: () True positive: \u0009 $truePositive \u0009 [Click predicted and click done] > To maximize")

    val trueNegative = labelPrediction
      .filter(col(predictionColumn) === 0.0) // Predicted not clicked
      .filter(col(labelColumn) === 0.0) // Actually not clicked
      .count()
    println(s":: () True negative: \u0009  $trueNegative \u0009 [Click NOT predicted and click NOT done] > To maximize")

    val falsePositive = labelPrediction
      .filter(col(predictionColumn) === 1.0) // Predicted clicked
      .filter(col(labelColumn) === 0.0) // Actually not clicked
      .count()
    println(s":: () False positive: \u0009 $falsePositive \u0009 [Click predicted and click NOT done] > OSEF")

    val falseNegative = labelPrediction
      .filter(col(predictionColumn) === 0.0) // Predicted not clicked
      .filter(col(labelColumn) === 1.0) // Actually clicked
      .count()
    println(s":: () False negative: \u0009 $falseNegative \u0009 [Click NOT predicted and click done] > To minimize")

    println()

    val recall = if (truePositive + falseNegative > 0) truePositive / (truePositive + falseNegative).asInstanceOf[Double] else 0
    println(s":: () Recall: \u0009\u0009\u0009 $recall")

    val precision = if (truePositive + falsePositive > 0) truePositive / (truePositive + falsePositive).asInstanceOf[Double] else 0
    println(s":: () Precision: \u0009\u0009 $precision")
  }
}
