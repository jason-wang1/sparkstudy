package com.ml.charpter2

import java.util

import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class LabelWords(label: Integer, words: Array[String])

/**
  * Descreption:
  * 采用ML Pipelines构建一个文档分类器，需要将模型进行保存，并且加载模型后对测试样本进行预测，考查点：
  * 1）	spark读取文件
  * 2）	数据清洗，考查Datasets的基本操作
  * 3）	构建分类器的管道，考查构建各种转换操作
  * 4）	读取模型，读取测试数据，并且进行模型测试
  *
  * Date: 2020年05月13日
  *
  * @author WangBo
  * @version 1.0
  */
object homeworkTransformers {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("homeworkTransformers").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 1.读取训练数据，清洗数据
    val url = this.getClass.getResource("data/doc_class.dat")
    val documentDS: RDD[String] = spark.read.textFile(url.getPath).rdd.cache()
    val title: String = documentDS.first()

    val labelWords: Dataset[LabelWords] = documentDS
      .filter(!title.contains(_))
      .map(line => {
        val featureLabels: Array[String] = line.split("\\|")
        val label: String = featureLabels(1)
        val feature: String = featureLabels(4)
        val words: Array[String] = feature.split(", ")
        LabelWords(Integer.parseInt(label), words)
      }).toDS().cache()

    val wordsAll: RDD[(String, Long)] = labelWords.select($"words")
      .rdd
      .flatMap(row => row.getSeq(0))
      .distinct()
      .zipWithIndex()

    val wordsIndexMap: collection.Map[String, Long] = wordsAll.collectAsMap()

    val data: DataFrame = labelWords
      .map(labelWords => {
        val words: Array[String] = labelWords.words
        val vector: Array[Double] = createVector(words, wordsIndexMap)
        (labelWords.label, Vectors.dense(vector))
      }).toDF("label", "features").cache()

    val trainingAndTest: util.List[DataFrame] = data.randomSplitAsList(Array(0.9, 0.1), 1L)

    val training: DataFrame = trainingAndTest.get(0).toDF()
    val test: DataFrame = trainingAndTest.get(1)

    // 2.创建逻辑回归Estimator
    val lr = new LogisticRegression()

    // 3.设置训练参数
    val paramMap: ParamMap = new ParamMap().put(lr.maxIter -> 30, lr.regParam -> 0.1)

    // 4.训练模型
    val model: LogisticRegressionModel = lr.fit(training, paramMap)
    val predict: DataFrame = model.transform(test)
    predict.show()

    DenseVector.zeros[Double](1)

  }

  /**
    * 根据词典，将词项集合转换为一个稀疏行向量，数值为0或1
    * @param words 词项集合
    * @param wordsIndexMap 词典
    */
  def createVector(words: Array[String], wordsIndexMap: collection.Map[String, Long]) = {
    var index = 0
    val x = new Array[Double](wordsIndexMap.size)
    words.foreach(word => {
      index = wordsIndexMap.getOrElse(word, 0L).toInt
      x(index) = 1.0
    })
    x
  }
}
