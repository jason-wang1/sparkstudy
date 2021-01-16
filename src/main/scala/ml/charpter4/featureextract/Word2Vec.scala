package com.ml.charpter4.featureextract

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月14日
  *
  * @author WangBo
  * @version 1.0
  */
object Word2Vec {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("CrossValidation").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    documentDF.show()

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result: DataFrame = model.transform(documentDF).cache()
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
    result.show(false)
  }
}
