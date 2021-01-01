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
 * Descreption:
 *
 * 采用ML Pipelines构建一个文档分类器，需要将模型进行保存，并且加载模型后对测试样本进行预测，考查点：
 * 1）	数据清洗，把样本转换为结构化的DataFrame，划分训练集与测试集
 * 2）	构建分类器的管道Pipeline：1.分词；2.计算词频
 * 3）	通过交叉验证CrossValidator训练模型
 * 4)   在测试集上验证模型效果
 *
 * 数据集描述：
 * myapp_id：文档id
 * typenameid：文档类别id，即需要预测的标签
 * typename：文档类别
 * myapp_word：部分文档内容
 * myapp_word_all：全部文档内容，即用于训练的特征
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
