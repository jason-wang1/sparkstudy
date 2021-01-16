package com.qf.day11

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 以下代码会出现一个问题：
  * list是在Driver端创建的，但是因为需要在Executor端使用，
  * 所以Driver会将list以task的形式发送到Executor端
  * 也就是相当于在Executor端需要复制一份，
  * 如果有很多很多个task，就会有很多的task在Executor端携带很多个list
  * 如果这个list数据非常大的时候，就可能会造成oom
  */
object BroadcastDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastDemo1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 该变量的值是在Driver端
    val list = List("hello", "hanmeimei", "mimi")

    val lines = sc.textFile("hdfs://node01:9000/files")
    val filtered = lines.filter(list.contains(_))

    filtered.foreach(println)

    sc.stop()
  }
}
