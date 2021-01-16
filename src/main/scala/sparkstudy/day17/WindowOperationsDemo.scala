package com.sparkstudy.day17

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Descreption:
  * 用窗口操作的方式进行单词计数
  * 窗口大小12秒，滑动间隔6秒
  */
object WindowOperationsDemo {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val conf: SparkConf = new SparkConf().setAppName("WindowOperation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(3))

    //获取netcat的数据
    val msg: ReceiverInputDStream[String] = ssc.socketTextStream("NODE01", 9999)

    //统计
    val tup: DStream[(String, Int)] = msg.flatMap(_.split(" ")).map((_, 1))

    val windowDstream: DStream[(String, Int)] =
    tup.window(Durations.seconds(6), Durations.seconds(3))

    msg
    val sumed1: DStream[(String, Int)] = windowDstream.reduceByKey(_+_)

    val sumed2: DStream[(String, Int)] =
      tup.reduceByKeyAndWindow((x: Int, y: Int) => x+y, Durations.seconds(12), Durations.seconds(6))

    //打印
    sumed1.print()
//    sumed2.print()

    //提交任务
    ssc.start()
    ssc.awaitTermination()
  }
}
