package com.qf.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化环境(模板代码)
      */
    // Spark的配置类，可以灵活的配置用于运行应用程序的一些必要的配置，比如调用什么压缩格式，资源的分配比例等等
    val conf: SparkConf = new SparkConf()
    // 指定应用程序名称，可以不指定，会生成一个很长的字符串
    conf.setAppName("SparkWC")
    // 指定本地模式运行（local模式）
    // 其中local是指调用一个线程运行任务
    // local[2]是指调用两个线程运行任务
    // local[*]是指有多少空闲的线程就用多少
    conf.setMaster("local[2]")
    // 用于提交任务的入口类，也叫上下文对象
    val sc: SparkContext = new SparkContext(conf)

    // 获取数据
    val lines: RDD[String] = sc.textFile("hdfs://node01:9000/files")

    // 分析数据
    val words: RDD[String] = lines.flatMap(_.split(" ")) // 切分生成一个个单词
    val tuples: RDD[(String, Int)] = words.map((_, 1)) // 生成一个个元组
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_ + _) // 进行聚合
    val sorted: RDD[(String, Int)] = sumed.sortByKey(false)

    // 结果的存储或展示
//    println(sorted.collect.toBuffer)
    sorted.foreach(println)
//    sorted.saveAsTextFile("hdfs://node01:9000/out-20190702-2")

    // 释放对象
    sc.stop()
  }
}
