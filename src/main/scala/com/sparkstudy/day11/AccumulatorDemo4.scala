package com.sparkstudy.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月11日
  *
  * @author WangBo
  * @version 1.0
  */
object AccumulatorDemo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("AccumulatorWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("D://data/test.txt")

    val acc = new MyAccumuletor2

    //注册


  }

}
