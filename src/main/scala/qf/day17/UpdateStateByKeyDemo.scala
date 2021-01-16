package com.qf.day17

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 将历史批次数据应用到当前批次进行聚合
  */
object UpdateStateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("updatestatebykey").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.milliseconds(5000))

    ssc.checkpoint("d://cp-20190716-1")

    // 获取数据
    val msg = ssc.socketTextStream("NODE01", 9999)
    val tup: DStream[(String, Int)] = msg.flatMap(_.split(" ")).map((_, 1))
    // updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)], 用于将历史数据和当前批次数据进行计算的函数
    // partitioner: Partitioner, 指定分区器
    // rememberPartitioner: Boolean 是否记录分区信息

    val sumed: DStream[(String, Int)] = tup.updateStateByKey { case (seq, buffer) => {
      val sumed: Int = buffer.getOrElse(0) + seq.sum
      Option(sumed)
    }
    }

    sumed.print()

    ssc.start()
    ssc.awaitTermination()
  }

  // (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
  // 其中：K代表数据中的key
  // Seq[V]代表当前批次单词出现的次数： Seq(1,1,1,1,.....)
  // Option[S]代表历史批次累加的结果，可能有值（Some），也可能没有值（None），获取数据的时候可以用getOrElse
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    // x代表it中的其中一个元素，元素就是一个元组
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

}
