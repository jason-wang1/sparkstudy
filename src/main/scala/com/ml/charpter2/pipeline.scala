package com.ml.charpter2

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月13日
  *
  * @author WangBo
  * @version 1.0
  */
object pipeline {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("charpter2").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //1 训练样本
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0))).toDF("id", "text", "label")

    //2 ML pipeline 参数设置, 包括三个过程: 首先是tokenizer, 然后是hashingTF, 最后是lr。
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // 用训练样本拟合pipeline
    val model = pipeline.fit(training)

    model.transform(training).show()

//    //4 保存pipeline模型
//    model.write.overwrite().save("C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\model")
//    //5 保存pipeline
//    pipeline.write.overwrite().save("/tmp/unfit-lr-model")
//    //6 加载pipeline模型
//    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

  }
}
