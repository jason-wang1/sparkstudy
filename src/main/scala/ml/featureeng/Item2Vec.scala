package com.ml.featureeng

import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Descreption: XXXX<br/>
  * Date: 2020年11月25日
  *
  * @author WangBo
  * @version 1.0
  */
object Item2Vec {

  def trainItem2Vec(spark: SparkSession, samples: RDD[Seq[String]], embLen: Int): Unit = {
    val word2vec = new Word2Vec()
      // 注意这几个超参数
      .setVectorSize(embLen)
      .setWindowSize(5)
      .setNumIterations(10)

    val model = word2vec.fit(samples)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("Item2Vec")
      .setMaster("local[*]")

    val spark = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()

    val embLen = 10

    val samples = getSamples(spark)

    trainItem2Vec(spark, samples, embLen)
  }

  def getSamples(spark: SparkSession) = {

    val filterSingleEle = udf{ arr: Seq[String] =>
      arr.length > 2
    }

    val rmDupEles = udf{ arr: Seq[String] =>
      val resArr = new ListBuffer[String]()
      var str = ""
      for (elem <- arr) {
        if (!elem.equals(str)){
          str = elem
          resArr.append(elem)
        }
      }
      resArr.toArray
    }

    import spark.implicits._

    val df: DataFrame = Seq(
      ("Bob", Array("F", "E", "C")),
      ("Ali", Array("C", "A", "E")),
      ("Tom", Array("D")),
      ("Zoe", Array("B", "B", "D"))
    ).toDF("userId", "itemList")

    df.where(filterSingleEle($"itemList"))
      .withColumn("itemList", rmDupEles($"itemList"))
      .select($"itemList")
      .rdd
      .map(row => row.getSeq[String](0))
  }
}
