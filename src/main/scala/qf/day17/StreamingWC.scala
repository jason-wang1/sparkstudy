package com.qf.day17

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * 获取NetCat的数据并统计WordCount
  */
object StreamingWC {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Durations.seconds(5))

    // 获取NetCat的数据
    val msg: ReceiverInputDStream[String] = ssc.socketTextStream("NODE01", 9999)

    // 统计
    val sumed: DStream[(String, Int)] = msg.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    // 打印
    sumed.print()

    // 提交任务到集群
    ssc.start()
    // 线程等待，等待处理任务
    ssc.awaitTermination()
  }
}
