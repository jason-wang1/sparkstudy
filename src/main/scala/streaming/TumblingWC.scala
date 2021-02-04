package streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输入：实时输入英文文本
  * 输出：实时滚动统计最近5秒的 word count
  *
  * 前提：在本地上打开终端，启动9999端口（命令：nc -lk 9999）
  */
object TumblingWC {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf: SparkConf = new SparkConf().setAppName("TumblingWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    // 实时获取netcat的数据
    val msg: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //统计
    //这里的flatMap是org.apache.spark.streaming.dstream包的DStream类的方法
    val sumed: DStream[(String, Int)] = msg.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_+_)

    //打印
    sumed.print()

    //提交任务
    ssc.start()
    ssc.awaitTermination()
  }
}
