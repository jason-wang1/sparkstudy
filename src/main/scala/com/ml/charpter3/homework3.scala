package com.ml.charpter3


import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月13日
  *
  * @author WangBo
  * @version 1.0
  */
object homework3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("homework").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 1.读取训练数据，清洗数据
    val url = this.getClass.getResource("/data/doc_class.dat")
    val documentDS: RDD[String] = spark.read.textFile(url.getPath).rdd.cache()
    val title: String = documentDS.first()

    val trainingAndTest: Array[DataFrame] = documentDS
      .filter(!title.contains(_))
      .map(line => {
        val featureLabels: Array[String] = line.split("\\|")
        val label: String = featureLabels(1)
        val feature: String = featureLabels(4)
        (Integer.parseInt(label).toDouble, feature)
      }).toDF("label", "text")
      .randomSplit(Array(0.9, 0.1), 1)

    val training: DataFrame = trainingAndTest(0)
    val test: DataFrame = trainingAndTest(1)

    // 2.建立ML管道，三步：tokenizer、hashingTF、lr
    val tokenizer: RegexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern(", ")
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(30)
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    val pipeline2: Pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF))
    val model: PipelineModel = pipeline2.fit(training)
    model.transform(training).show(false)

//    // 3.设定参数进行网格搜索
//    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
//      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
//      .addGrid(lr.regParam, Array(0.1, 0.01))
//      .build()
//
//    // 4.交叉验证
//    val cv: CrossValidator = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(new BinaryClassificationEvaluator())
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(3)
//    val cvModel: CrossValidatorModel = cv.fit(training)
//
//    // 5. 在测试集预测
//    cvModel.transform(test)
//      .show(100)



  }
}
