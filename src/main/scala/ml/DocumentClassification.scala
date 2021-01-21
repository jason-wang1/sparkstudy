package ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{RegexTokenizer, Word2Vec}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: 文本分类
  * 特征工程：分词；word2vec
  * 分类模型：随机森林
  *
  * 数据集描述：
  * myapp_id：文档id
  * typenameid：文档类别id，即需要预测的标签
  * typename：文档类别
  * myapp_word：部分文档内容
  * myapp_word_all：全部文档内容，即用于训练的特征
  *
  * 构建机器学习模型的范本
  */
object DocumentClassification {
  val saveModelPath: String = "/Users/wangbo/IdeaProjects/sparkstudy/src/main/resources/model.docclass_word2vec_rf"

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
      if (init) modelTraining(spark, trainingData, saveModelPath)
      else CrossValidatorModel.load(saveModelPath)

    // 查看交叉验证模型选出的最佳超参数
    val param0: ParamMap = model.bestModel.asInstanceOf[PipelineModel].stages(0).extractParamMap()
    val param1: ParamMap = model.bestModel.asInstanceOf[PipelineModel].stages(1).extractParamMap()
    val param2: ParamMap = model.bestModel.asInstanceOf[PipelineModel].stages(2).extractParamMap()
    println(s"The best tokenizer param are \n$param0")
    println(s"The best word2vec param are \n$param1")
    println(s"The best rf param are \n$param2")

    // 用测试集验证模型
    modelEvaluating(model, testData)
  }

  def readData(spark: SparkSession) = {
    import spark.implicits._
    // 读取训练数据，清洗数据
    val url = this.getClass.getResource("/data/doc_class.dat")
    val documentDS: RDD[String] = spark.read.textFile(url.getPath).rdd.cache()
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

  def modelTraining(spark: SparkSession, trainingData: DataFrame, saveModelPath: String) = {

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

    model.write.overwrite().save(saveModelPath)
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
