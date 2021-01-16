package com.qf.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * 需求二：统计每个省份每个小时的广告ID的top3
  */
object AdventProAndHourTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdventProAndHourTop3").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据
    val logs = sc.textFile("D://data/Advert.log")

    // 切分
    val logsArray: RDD[Array[String]] = logs.map(_.split("\t"))

    // 生成数据粒度: (pro_hour_ad, 1)
    val pro_hour_adAndOne: RDD[(String, Int)] =
      logsArray.map(x => (x(1) + "_" + getHour(x(0)) + "_" + x(4), 1))

    // 计算每一个省份每个小时每个广告的点击量
    val pro_hour_ad2sum: RDD[(String, Int)] = pro_hour_adAndOne.reduceByKey(_ + _)

    // 拆分pro_hour_adid, 生成数据粒度：(pro_hour, (adid, sum))
    val pro_hour2AdArray: RDD[(String, (String, Int))] = pro_hour_ad2sum.map(x => {
      val fields = x._1.split("_")
      (fields(0) + "_" + fields(1), (fields(2), x._2))
    })
    pro_hour2AdArray

    // 将省份和小时内的数据进行分组
    val pro_hour2AdGroup: RDD[(String, Iterable[(String, Int)])] =
      pro_hour2AdArray.groupByKey()

    // 组内排序并取top3
    val pro_hour2Top3AdArr: RDD[(String, List[(String, Int)])] =
      pro_hour2AdGroup.mapValues(_.toList.sortWith(_._2 > _._2).take(3))

    // 整合数据，生成粒度为：(pro,(hour, Array[(adid, sum)]))
    val pro2hourAdArr: RDD[(String, (String, List[(String, Int)]))] = pro_hour2Top3AdArr.map(x => {
      val fields = x._1.split("_")
      (fields(0), (fields(1), x._2))
    })

    // 按照省份进行分组
    val res: RDD[(String, Iterable[(String, List[(String, Int)])])] =
      pro2hourAdArr.groupByKey()

    println(res.collect.toBuffer)


    sc.stop()
  }

  /**
    * 返回时间戳的小时
    *
    * @param time_long
    * @return
    */
  def getHour(time_long: String): String = {
    val time = new DateTime(time_long.toLong)
    time.getHourOfDay.toString
  }
}
