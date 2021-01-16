package com.sparkstudy.day17

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.concurrent.duration.Duration

/**
  * Descreption:
  * 获取NetCat的数据并统计WordCount
  * 前提：在NODE01上打开netcat（命令：nc -lk 9999）
  */
object StreamingWC {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val conf: SparkConf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    //获取netcat的数据
    val msg: ReceiverInputDStream[String] = ssc.socketTextStream("NODE01", 9999)

    //统计
    //这里的flatMap是org.apache.spark.streaming.dstream包的DStream类的方法
    val sumed: DStream[(String, Int)] = msg.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    //打印
    sumed.print()

    //提交任务
    ssc.start()
    ssc.awaitTermination()
  }
}
