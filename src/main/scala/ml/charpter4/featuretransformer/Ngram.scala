package com.ml.charpter4.featuretransformer

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月14日
  *
  * @author WangBo
  * @version 1.0
  */
object Ngram {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("CrossValidation").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("I", "heard", "about", "Spark"))
//      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
//      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame
//      .select("ngrams")
      .show(false)
  }
}
