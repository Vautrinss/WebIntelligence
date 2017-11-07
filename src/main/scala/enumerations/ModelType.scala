package enumerations

object ModelType extends Enumeration {
  val DECISION_TREE: ModelType.Value = Value("DecisionTree")
  val RANDOM_FOREST: ModelType.Value = Value("RandomForest")
  val LOGISTIC_REGRESSION: ModelType.Value = Value("LogisticRegression")
  val GRADIENT_BOOSTED_TREE: ModelType.Value = Value("GradientBoostedTree")
}
