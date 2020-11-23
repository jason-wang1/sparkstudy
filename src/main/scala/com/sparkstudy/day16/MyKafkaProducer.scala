package com.sparkstudy.day16

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Descreption: 创建kafka生产者
  * Date: 2019年07月18日
  *
  * @author WangBo
  * @version 1.0
  */
object MyKafkaProducer {
  def main(args: Array[String]): Unit = {
    val topic1 = "test1"
    val topic2 = "test2"

    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092")
    props.put("retries", "3")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer1: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    val producer2: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    for (i <- 1 to 100) {
      val msg1 = s"$i producer send msg"
      val j = 101-i
      val msg2 = s"$j producer send msg"

      producer1.send(new ProducerRecord[String, String](topic1, i.toString, msg1))
      producer2.send(new ProducerRecord[String, String](topic2, i.toString, msg2))

      Thread.sleep(500)
    }

    producer1.close()
    producer2.close()
  }
}
