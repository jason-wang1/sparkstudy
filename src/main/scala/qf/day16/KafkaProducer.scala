package com.qf.day16

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Description：创建kafka生产者<br/>
  * Copyright (c) ， 2019， John Will <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年07月15日
  *
  * @author LongZheng
  * @version : 1.0
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val topic = "test2"
    val topic1 = "test3"
    val props = new Properties()
    //指定broker的地址清单，格式 host:port。清单里不需要包含所有的broker 地址，
    //生产者会从给定的 broker 里查找到其他 broker 的信息。不过建议至少要提供两个 broker 信息，
    //且其中一个若机，生产者仍然能够连接到集群上。
    props.put("bootstrap.servers", "NODE01:9092,NODE02:9092")
    //请求失败时的重试次数
    props.put("retries", "3")
    //生产者使用这个类把key对象序列化成字节数组
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //生产者使用这个类把value对象序列化成字节数组
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //创建生产者对象
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    val producer1: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    //模拟将数据发送给 kafka的topic中

    for (i <- 1 to 10000) {
      val msg = s"$i : producer send msg"

      producer.send(new ProducerRecord[String, String](topic, msg))
      producer.send(new ProducerRecord[String, String](topic1, msg))

      Thread.sleep(500)

    }
    producer.close()
    producer1.close()

  }
}
