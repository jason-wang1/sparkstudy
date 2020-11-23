package com.qf.day11

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用累加器实现单词计数
  */
object AccumulatorDemo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo4").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val hashAcc = new MyAccumuletor2
    sc.register(hashAcc, "wc")

    val data = sc.textFile("D://data/test.txt").flatMap(_.split(" "))

    data.foreach(word => hashAcc.add(word))

    println(hashAcc.value)

    sc.stop()
  }
}
