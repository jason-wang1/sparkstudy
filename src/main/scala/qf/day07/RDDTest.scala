package com.qf.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rddtest").setMaster("local")
    val sc = new SparkContext(conf)

    // 创建RDD的两种方式
    // 其中一种是通过外部存储介质获取数据并生成RDD
    val lines = sc.textFile("hdfs://node01:9000/files", 10)

    // 另一种是通过并行化的方式生成RDD
    val arr = Array(1,2,3,4,5,6)
    val rdd: RDD[Int] = sc.parallelize(arr)
    val rdd2: RDD[Int] = sc.makeRDD(arr)



  }
}
