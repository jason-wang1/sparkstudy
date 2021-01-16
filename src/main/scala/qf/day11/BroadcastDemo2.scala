package com.qf.day11

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用广播变量
  */
object BroadcastDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastDemo1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val list = List("hello")

    // 封装广播变量,相当于进行广播, 在Driver端执行
    val broadcast: Broadcast[List[String]] = sc.broadcast(list)

    val lines = sc.textFile("hdfs://node01:9000/files")
    // 在Executor端执行, broadcast.value就是从本地拿取的广播过来的值
    val filtered = lines.filter(broadcast.value.contains(_))
    filtered.foreach(println)

    sc.stop()
  }
}
