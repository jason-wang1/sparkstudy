package com.ml.charpter2

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月13日
  *
  * @author WangBo
  * @version 1.0
  */
object transformers {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("charpter2").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //1 训练样本
    val training: DataFrame = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5)))).toDF("label", "features")
    training.show()

    //2 创建逻辑回归Estimator
    val lr = new LogisticRegression()
    println(s"LogisticRegression parameters:\n ${lr.explainParams()} \n")

    //3 通过setter方法设置模型参数
    lr.setMaxIter(10)
      .setRegParam(0.01)

    //4 训练模型
    val model1: LogisticRegressionModel = lr.fit(training)
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

    //5 通过ParamMap设置参数方法，这种方式会覆盖之前设置的参数
    val paramMap: ParamMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter -> 30)
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

    //5 ParamMap合并
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ paramMap2

    //6 训练模型，采用paramMap参数
    val model2: LogisticRegressionModel = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    //7 测试样本
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5)))).toDF("label", "features")

    //8 对模型进行测试
    val result: DataFrame = model2.transform(test)
//      .select("features", "label", "myProbability", "prediction")
    result.show()

  }
}
