package streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输入：实时输入英文文本
  * 输出：word count，窗口大小为6秒，滑动间隔为3秒
  *
  * 前提：在本地上打开终端，启动9999端口（命令：nc -lk 9999）
  */
object SlidingWindowWC {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf: SparkConf = new SparkConf().setAppName("WindowOperation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(3))

    // 获取netcat的数据
    val msg: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 统计
    val tup: DStream[(String, Int)] = msg.flatMap(_.split("\\W+")).map((_, 1))

    val windowDstream: DStream[(String, Int)] =
      tup.window(Durations.seconds(6), Durations.seconds(3))

    val summed: DStream[(String, Int)] = windowDstream.reduceByKey(_+_)

    // 打印
    summed.print()

    //提交任务
    ssc.start()
    ssc.awaitTermination()
  }
}
