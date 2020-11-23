package com.qf.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求: 求所有用户经过的所有基站停留时长最长的top2
  * 思路：
  *   1、获取用户交互数据并切分
  *   2、求用户在各基站停留的总时长
  *   3、获取基站的基础信息
  *   4、将经纬度信息join到用户的访问数据中
  *   5、求用户在各基站停留时间的top2
  */
object LacLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LacLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取用户访问数据
    val fileInfo = sc.textFile("D://data/lacduration/log")
    // 切分
    val userInfo: RDD[((String, String), Long)] = fileInfo.map(line => {
      val fields = line.split(",")
      val phone = fields(0) // 用户手机号
      val time = fields(1).toLong // 时间戳
      val lac = fields(2) // 基站id
      val eventType = fields(3) // 建立会话和断开会话字段（事件类型）
      val time_long = if (eventType.equals("1")) -time else time

      ((phone, lac), time_long) // 这样返回数据，便于我们接下来的聚合统计
    })
    // 用户在相同基站停留的总时长
    val phoneAndLac2Sum: RDD[((String, String), Long)] = userInfo.reduceByKey(_+_)

    // 为了便于和基站信息进行join，需要将数据进行调整
    // 将基站id作为key，手机号和时长作为value
    val lacAndPhoneAndTime: RDD[(String, (String, Long))] = phoneAndLac2Sum.map(tup => {
      val phone = tup._1._1 // 手机号
      val lac = tup._1._2 // 基站id
      val time = tup._2 // 用户在某个基站停留的总时长

      (lac, (phone, time))
    })

    // 获取基站信息并切分
    val lacInfo = sc.textFile("D://data/lacduration/lac_info.txt").map(line => {
      val fields = line.split(",")
      val lac = fields(0)
      val x = fields(1) // 经度
      val y = fields(2) // 纬度
      (lac, (x, y))
    })

    // 将经纬度信息join到用户访问信息中
    val joined: RDD[(String, ((String, Long), (String, String)))] =
      lacAndPhoneAndTime.join(lacInfo)

    // 为了便于以后的分组组内排序，需要进行数据整合
    val phoneAndTimeAndXY: RDD[(String, Long, (String, String))] = joined.map(tup => {
      val phone = tup._2._1._1 // 手机号
      val time = tup._2._1._2 // 时长
      val xy = tup._2._2 // 经纬度

      (phone, time, xy)
    })

    // 按照用户的手机号进行分组
    val grouped: RDD[(String, Iterable[(String, Long, (String, String))])] =
      phoneAndTimeAndXY.groupBy(_._1)

    // 按照时长进行组内降序排序
    val sorted: RDD[(String, List[(String, Long, (String, String))])] =
      grouped.mapValues(_.toList.sortWith(_._2 > _._2))

    // 整合数据
    val filtered: RDD[(String, List[(Long, (String, String))])] = sorted.map(tup => {
      val phone = tup._1
      val list = tup._2
      val filterList = list.map(t => {
        val time = t._2
        val xy = t._3
        (time, xy)
      })

      (phone, filterList)
    })

    // 取top2
    val res = filtered.mapValues(_.take(2))

    println(res.collect.toBuffer)

    sc.stop()
  }
}
