package com.qf.day17

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import java.lang

/**
  * 需求：获取Kafka的数据并进行单词统计
  *   在这个过程熟悉用Direct方式消费数据并查看offset
  */
object LoadKafkaDataDemo {
  def main(args: Array[String]): Unit = {
    val cpdir = "d://cp-20190716-3"

    // 初始化环境
    val ssc = StreamingContext.getOrCreate(cpdir, () => createContext())
    ssc.start()
    ssc.awaitTermination()
  }

  // 该方法创建上下文并计算，返回计算后的上下文
  def createContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("LoadKafkaDataDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 需要checkpoint
    ssc.checkpoint("d://cp-20190716-4")

    val topics = Array("test1")

    val kafkaParams = Map[String, Object](
      // 指定请求kafka的集群列表
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      // 获取数据时用的key的解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      // 获取数据时用的value的解码方式
      "value.deserializer" -> classOf[StringDeserializer],
      // 指定消费者组
      "group.id" -> "group1",
      // 消费位置
      "auto.offset.reset" -> "latest",
      // 如果value合法，自动提交offset
      "enable.auto.commit" -> (true: lang.Boolean)
    )

    // 调用Streaming提供的工具类的直连方式来消费kafka数据
    val msgs: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
      ssc,

      /**
        * Spark  2.x  kafka  LocationStrategies 的几种方式。
        * 1. LocationStrategies.PreferBrokers()
        * 仅仅在你 spark 的 executor 在相同的节点上，优先分配到存在  kafka broker 的机器上；
        * 2. LocationStrategies.PreferConsistent();
        * 大多数情况下使用，一致性的方式分配分区所有 executor 上。（主要是为了分布均匀）
        * 3. LocationStrategies.PreferFixed(hostMap: collection.Map[TopicPartition, String])
        * 4. LocationStrategies.PreferFixed(hostMap: ju.Map[TopicPartition, String])
        * 如果你的负载不均衡，可以通过这两种方式来手动指定分配方式，其他没有在 map 中指定的，均采用 preferConsistent() 的方式分配；
        */
      LocationStrategies.PreferConsistent,
      // 指定订阅主题
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )

    // 查看数据信息
    msgs.foreachRDD(rdd => {
      // 获取offset集合
      val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // foreachPartition在做结果数据的持久化时经常用到，
      // 主要是用于持久化的优化，可以大大减少对应的数据库的连接
      rdd.foreachPartition(part => {
        part.foreach(line => {
          // 获取到每个分区对应的offset的信息
          val o: OffsetRange = offsetsList(TaskContext.get().partitionId())
          println("=======topic:" + o.topic)
          println("=======partition:" + o.partition)
          println("=======fromOffset:" + o.fromOffset)
          println("=======topicPartition:" + o.topicPartition())
          println(line)
        })
      })
    })

    // 单词计数
    // 因为DStream里的key是offset，这里不需要，所以只需要将value取出即可
    val lines: DStream[String] = msgs.map(_.value())
    val tup: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    val partioner = new HashPartitioner(ssc.sparkContext.defaultParallelism)
    val sumed: DStream[(String, Int)] = tup.updateStateByKey(func, partioner, true)
    sumed.print()

    ssc
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map{
      case (x, y, z) => (x, y.sum + z.getOrElse(0))
    }
  }
}
