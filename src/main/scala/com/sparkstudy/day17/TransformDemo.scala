package com.sparkstudy.day17

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Descreption:
  * 获取NetCat的数据并统计WordCount
  */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val conf: SparkConf = new SparkConf().setAppName("TransformDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    //获取netcat的数据
    val msg: ReceiverInputDStream[String] = ssc.socketTextStream("NODE01", 9999)

    //调用transform进行统计
    //这里的flatmap方法是org.apache.spark.rdd包的RDD类的方法
    val sumed: DStream[(String, Int)] = msg.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })

    //打印
    sumed.print()

    //提交任务
    ssc.start()
    ssc.awaitTermination()
  }
}
