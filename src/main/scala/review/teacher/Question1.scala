package com.review.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object Question1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName(s"${this.getClass.getCanonicalName}")
    val sc = new SparkContext(conf)
//    sc.setLogLevel("error")

    val random = scala.util.Random
    val arr: immutable.IndexedSeq[Int] = for (i <- 1 to 1000000) yield {
      random.nextInt(100000)
    }

    val rdd = sc.makeRDD(arr)

    rdd.sortBy(x=>x)



    Thread.sleep(1000000)

    sc.stop()
  }
}

// 三种：hash、range、自定义
// 所有spark的Transformation算子都不会触发job