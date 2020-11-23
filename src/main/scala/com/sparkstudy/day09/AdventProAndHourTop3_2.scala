package com.sparkstudy.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Descreption:
  * 需求二：统计每个省份每个小时的广告ID的top3
  * 1562085629599	Hebei	Shijiazhuang	 564	  1
  * 时间戳         省份  城市          用户id  广告id
  * Date: 2019年07月07日
  *
  * @author WangBo
  * @version 1.0
  */
object AdventProAndHourTop3_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("AdvertProAndHour").setMaster("local")
    val sc = new SparkContext(conf)

    //获取数据
    val logs: RDD[String] = sc.textFile("D://data/Advert.log")

    //切分拼接数据
    val proHourAdid: RDD[(String, Int)] = logs.map(f = x => {
      val fields: Array[String] = x.split("\t")
      val pro: String = fields(1)
      val hour: String = getHour(fields(0))
      val adid: String = fields(4)
      (pro + "\t" + hour + "\t" + adid, 1)
    })

//    //聚合
//    val grouped: RDD[(String, Iterable[(String, Int)])] = proHourAdid.groupBy(_._1)
//    //求和
//    val sumed2: RDD[(String, Int)] = grouped.mapValues(_.size)

    //求和，一步到位，上面要两步
    val sumed: RDD[(String, Int)] = proHourAdid.reduceByKey(_ + _)

    //拆分
    val splitedSumed: RDD[(String, (String, Int))] = sumed.map(x => {
      val fields: Array[String] = x._1.split("\t")
      val pro: String = fields(0)
      val hour: String = fields(1)
      val adid: String = fields(2)
      (pro + "_" + hour, (adid, x._2))
    })

    //以省份与小时分组
//    val grouped1: RDD[(String, Iterable[(String, (String, Int))])] = splitedSumed.groupBy(_._1)
    val grouped: RDD[(String, Iterable[(String, Int)])] = splitedSumed.groupByKey()

    //排序
    val sorted: List[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortWith(_._2 >_._2).take(3)).collect().toList
    println(sorted)

    //排序（其他方法）
    val sorted2: List[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3)).collect().toList
    println(sorted2)

    sc.stop()

  }

  def getHour(time_string: String): String = {
    val time: DateTime = new DateTime(time_string.toLong)
    time.getHourOfDay.toString
  }

}
