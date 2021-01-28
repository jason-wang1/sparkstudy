package com.sparkstudy.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption:
  * 需求: 求所有用户经过的所有基站停留时长最长的top2
  * 18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
  * 手机号       时间            基站id                          标识符（1 建立连接；0 断开连接）
  *
  * 思路：
  *   1、获取用户交互数据并切分
  *   2、求用户在各基站停留的总时长
  *   3、获取基站的id信息
  *   4、将经纬度信息join到用户的访问数据中
  *   5、求用户在各基站停留时间的top2
  */
object LacLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("LacLocation").setMaster("local")
    val sc = new SparkContext(conf)

    //获取数据
    val path = this.getClass.getResource("/data/lacduration/log").getPath
    val logs: RDD[String] = sc.textFile(path)

    //切分数据
    val phonelacTime_lone: RDD[(String, Long)] = logs.map(x => {
      val fields: Array[String] = x.split(",")
      val phonelac: String = fields(0) + "_" + fields(2)
      val time: Long = fields(1).toLong
      val flag: String = fields(3).trim
      val time_long: Long = if (flag == "1") -time else time
      (phonelac, time_long)
    })

    //聚合
    val grouped: RDD[(String, Iterable[Long])] = phonelacTime_lone.groupByKey()

    //计算累计时长
    val sumed: RDD[(String, Long)] = grouped.mapValues(_.sum)

    for (i <- sumed.collect().toList) println(i)

    //拆分；
    val splited: RDD[(String, (String, Long))] = sumed.map(x => {
      val fields: Array[String] = x._1.split("_")
      (fields(0), (fields(1), x._2))
    })

    //聚合
    val groupedPhone: RDD[(String, Iterable[(String, Long)])] = splited.groupByKey()

    //排序
    val sorted: RDD[(String, List[(String, Long)])] = groupedPhone.mapValues(_.toList.sortWith(_._2 > _._2).take(2))

    val res: List[(String, List[(String, Long)])] = sorted.collect().toList

    for (i <- res) println(i)

    sc.stop()

  }
}
