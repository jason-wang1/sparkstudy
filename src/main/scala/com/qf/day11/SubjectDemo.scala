package com.qf.day11

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 需求：实现一个学科对应一个结果文件
  */
object SubjectDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SubjectDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据,切分并生成一个个元组
    val tup = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/subjectaccess/access.txt")
      .map(line => {
        val fields = line.split("\t")
        (fields(1), 1)
      })

    // 将相同的url进行聚合，得到各学科的模块的访问量
    val sumed: RDD[(String, Int)] = tup.reduceByKey(_ + _).cache()

    // 解析出学科并返回
    val subjectAndUrlAndCount: RDD[(String, (String, Int))] = sumed.map(tup => {
      val url = tup._1
      val count = tup._2
      val subject = new URL(url).getHost

      (subject, (url, count))
    })

    // 取出所有的学科, 需要将重复的学科去重
    val subjectInfo: RDD[String] = subjectAndUrlAndCount.keys.distinct
    val subjectArr = subjectInfo.collect

    // 调用默认的分区器
//    val partitioner = new HashPartitioner(5)
//    val partitioned: RDD[(String, (String, Int))] = subjectAndUrlAndCount.partitionBy(partitioner)
//    partitioned.saveAsTextFile("d://res")

    // 调用自定义分区器
    val partitioner = new SubjectPartitioner(subjectArr)
    val partitioned: RDD[(String, (String, Int))] = subjectAndUrlAndCount.partitionBy(partitioner)

    // 取top3
    val res = partitioned.mapPartitions(it => {
      val list = it.toList
      val sorted = list.sortWith(_._2._2 > _._2._2)
      val top3 = sorted.take(3)
      top3.iterator
    })

    res.saveAsTextFile("d://res1")

    sc.stop()
  }
}

/**
  * 自定义分区器，按照一个学科一个分区的规则进行定义
  * @param subjects
  */
class SubjectPartitioner(subjects: Array[String]) extends Partitioner {
  // 创建一个map，用来存储学科对应的分区号 key=subject, value=分区号
  val subjectMap = new mutable.HashMap[String, Int]()
  // 定义一个计数器，用来生成分区号
  var i = 0

  for (subject <- subjects) {
    subjectMap += (subject -> i)
    i += 1 // 分区自增
  }

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = {
    subjectMap.getOrElse(key.toString, 0)
  }
}
