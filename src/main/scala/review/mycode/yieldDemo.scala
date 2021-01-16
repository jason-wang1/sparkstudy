package com.review.mycode

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object Question1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName(s"${this.getClass.getCanonicalName}")
    val sc = new SparkContext(conf)
//    sc.setLogLevel("error")

    val random = scala.util.Random
    val arr: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield {
      i
    }

    println(arr.toList)

    sc.stop()
  }
}

// 三种：hash、range、自定义
// 所有spark的Transformation算子都不会触发job