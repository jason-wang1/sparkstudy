package com.ml.charpter4

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: 文本分类
  * 标签：typenameid；特征：myapp_word_all
  *
  * Date: 2020年05月14日
  *
  * @author WangBo
  * @version 1.0
  */
object DocumentClassification {

  def main(args: Array[String]): Unit = {
    // 第一次训练设置为true，它会训练模型并保存模型
    // 之后设置为false，它会从之前保存的地方加载模型进行预测、评估等
    val init = false

    val sparkConf: SparkConf = new SparkConf().setAppName("DocumentClassification").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val data: DataFrame = readData(spark)

    val Array(trainingData, testData): Array[DataFrame] = data
      .randomSplit(Array(0.9, 0.1), 1)

    val model =
      if (init) modelTraining(spark, trainingData)
      else CrossValidatorModel.load("C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\model\\docclass_word2vec_rf")

    // 查看交叉验证模型选出的最佳超参数
    val param1: ParamMap = model.bestModel.asInstanceOf[PipelineModel].stages(1).extractParamMap()
    val param2: ParamMap = model.bestModel.asInstanceOf[PipelineModel].stages(2).extractParamMap()
    println(s"The best word2vec param are \n$param1")
    println(s"The best rf param are \n$param2")

    // 用测试集验证模型
    modelEvaluating(model, testData)
  }

  def readData(spark: SparkSession) = {
    import spark.implicits._
    // 读取训练数据，清洗数据
    val documentDS: RDD[String] = spark.read.textFile("C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\data\\doc_class.dat").rdd.cache()
    val title: String = documentDS.first()

    val data: DataFrame = documentDS
      .filter(!title.contains(_))
      .map(line => {
        val featureLabels: Array[String] = line.split("\\|")
        val label: String = featureLabels(1)
        val feature: String = featureLabels(4)
        (Integer.parseInt(label).toDouble, feature)
      }).toDF("label", "text")

    data
  }

  def modelTraining(spark: SparkSession, trainingData: DataFrame) = {

    // Transformer 特征转换器：分词器分词
    val tokenizer: RegexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern(", ")

    // Transformer 特征转换器：word2vec文档转向量，文档向量等于各个词向量之和
    val word2Vec: Word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinCount(0)

    // Estimator 模型学习器：随机森林
    val rf: RandomForestClassifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Pipeline：先把文档先分词，再转向量，作为特征输入到随机森林学习器
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(tokenizer, word2Vec, rf))

    // 设置多个备选超参数进行交叉验证
    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(word2Vec.vectorSize, Array(10, 20, 50))
      .addGrid(rf.numTrees, Array(5, 10, 20))
      .build()

    // 3折交叉验证
    val cv: CrossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    // 用训练数据训练模型，该模型会使用上面超参数的最佳组合
    val model: CrossValidatorModel = cv.fit(trainingData)

    model.write.overwrite().save("C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\model\\docclass_word2vec_rf")
    model
  }


  def modelEvaluating(model: CrossValidatorModel, testData: DataFrame) = {
    val predictions: DataFrame = model.transform(testData).cache()
    predictions.show(100)

    val accuracyEvaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy: Double = accuracyEvaluator.evaluate(predictions)
    println(s"accuracy is $accuracy")
  }
}
