package com.dataframe

import java.net.URL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Words(word: String, count: Int)

/**
  * Descreption: XXXX<br/>
  * Date: 2020年05月13日
  *
  * @author WangBo
  * @version 1.0
  */
object wc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val stopWords = Seq("a", "of", "to")

    val inputUrl: URL = this.getClass.getResource("/data/test.txt")
    val df: Dataset[String] = spark.read.textFile(inputUrl.getPath)
    df.rdd
      .flatMap(line => line.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map { case (key, value) => Words(key, value) }
      .toDS
      .show()

  }

}
