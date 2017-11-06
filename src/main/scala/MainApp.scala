import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions.desc

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler




object MainApp {
  def main(args: Array[String]) {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("WebIntelligence")
      .master("local[*]")
      .config("spark.executor.memory", "15g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df: DataFrame = spark.read.json("./data-students.json")
    //val df2 = df.filter("appOrSite is not null")

    //tranformor to convert string to category values
    val appOrSiteIndexer = new StringIndexer().setInputCol("appOrSite").setOutputCol("appOrSiteCat")
    val bidfloorIndexer = new StringIndexer().setInputCol("bidfloor").setOutputCol("bidfloorCat")
    val cityIndexer = new StringIndexer().setInputCol("city").setOutputCol("cityCat")
    val exchangeIndexer = new StringIndexer().setInputCol("exchange").setOutputCol("exchangeIndexer").fit(df)
    val impidIndexer = new StringIndexer().setInputCol("impid").setOutputCol("impidIndexer").fit(df)
    //val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndexer").fit(df)


    //val df4 = appOrSiteIndexer.transform(df)
    //df4.show()


    val assembler = new VectorAssembler().setInputCols(Array("appOrSiteCat",
      "bidfloorCat", "cityCat")).setOutputCol("features")
    //val df3 = assembler.transform(df)
    //df3.show()

    //df.groupBy("label", "appOrSite").count().orderBy(desc("count")).show()


  }
}

