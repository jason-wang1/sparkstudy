package com.sparkstudy.review.weektest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年08月16日
  *
  * @author WangBo
  * @version 1.0
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test2").setMaster("local")
    val sc = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("D://data/test2.txt")

    val containsInsertInto: RDD[String] = data.filter(_.contains("insert into"))
    val insertInto: (String, Int) = ("insert into", containsInsertInto.collect().size)

    val select: RDD[(String, Int)] = data.flatMap(_.split(" ")).filter(_.contains("select")).map((_, 1)).reduceByKey(_+_)

    select.collect().foreach(println)
    println(insertInto)

  }

}
