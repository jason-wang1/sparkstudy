package com.review.mycode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年08月14日
  *
  * @author WangBo
  * @version 1.0
  */
object WC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.textFile("D://data/text.txt")

    val result: RDD[(String, Int)] = text.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    result.foreach(println)

    val text2: Array[String] = Array[String]("hello zz hello hello hi", "mm", "zz", "hi hi hi")

    val result2: List[(String, Int)] = text2.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(_._1)
      .map(x => {
        (x._1, x._2.size)
      })
        .toList
        .sortWith(_._2 > _._2)

    println("*****************")
    result2.foreach(println)



  }

}
