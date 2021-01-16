package com.qf.day17

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用窗口操作的方式进行单词计数
  * 窗口大小10秒，滑动间隔10秒
  */
object WindowOperationsDemo {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf = new SparkConf().setAppName("WindowOperationsDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Durations.seconds(5))
    ssc.checkpoint("d://cp-20190716-1")

    // 获取数据
    val msg: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    // 统计
    val tup: DStream[(String, Int)] = msg.flatMap(_.split(" ")).map((_, 1))
    val sumed: DStream[(String, Int)] =
      tup.reduceByKeyAndWindow((x: Int, y: Int) => (x + y),
        Durations.seconds(10), Durations.seconds(5))

    val counted: DStream[Long] =
      tup.countByWindow(Durations.seconds(10), Durations.seconds(5))

    // 打印
    sumed.print()
    counted.print()

    // 提交任务到集群
    ssc.start()
    // 线程等待，等待处理任务
    ssc.awaitTermination()
  }
}
