package com.sparkstudy.day16

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年10月04日
  *
  * @author WangBo
  * @version 1.0
  */
object MyKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val topic1 = "test1"
    val topic2 = "test2"
    
    val props = new Properties()
    props.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put("group.id","group1")
    props.put("enable.auto.commit","true")
    props.put("auto.commit.interval.ms","1000")

    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    val consumer1: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    val consumer2: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    consumer1.subscribe(Collections.singleton("test1"))
    consumer2.subscribe(Collections.singleton("test1"))

    while (true) {
      val msgs1: ConsumerRecords[String, String] = consumer1.poll(10000)
//      val msgs2: ConsumerRecords[String, String] = consumer2.poll(10000)
      val it1: util.Iterator[ConsumerRecord[String, String]] = msgs1.iterator()
//      val it2: util.Iterator[ConsumerRecord[String, String]] = msgs2.iterator()

      while (it1.hasNext) {
        val msg1: ConsumerRecord[String, String] = it1.next()
        println("offset: "+msg1.offset()+"; key: "+msg1.key()+"; value: "+msg1.value())
      }

//      while (it2.hasNext){
//        val msg2: ConsumerRecord[String, String] = it2.next()
//        println("offset: "+msg2.offset()+"; key: "+msg2.key()+"; value: "+msg2.value())
//      }

    }
  }

}
