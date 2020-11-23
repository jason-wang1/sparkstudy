package com.qf.day16

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

/**
  * Description：创建kafka消费者<br/>
  * Copyright (c) ， 2019， John Will <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年07月15日
  *
  * @author LongZheng
  * @version : 1.0
  */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val props=new Properties()
    props.put("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.put("group.id","group1")
    props.put("enable.auto.commit","true")
    props.put("auto.commit.interval.ms","1000")

    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String,String](props)

    //开始 订阅topic,可以传一个，或者多个topic

    consumer.subscribe(Collections.singletonList("test3"))
    while (true){
      val msgs: ConsumerRecords[String, String] = consumer.poll(1000)
      val it=msgs.iterator()
      while (it.hasNext){
        val msg=it.next()
        println("offset"+msg.offset()+"key:"+msg.key()+"value:"+msg.value())
      }
    }
  }

}
