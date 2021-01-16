package com.qf.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据格式：共5个字段，以“\t”分割
  * timestamp province  city  userid  adid
  * 访问时间  省份        城市  用户id  广告id
  *
  * 需求一：统计每个省份广告点击top3
  */
object Advent_ProvinceTop3 {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val conf = new SparkConf()
      .setAppName("Advent_ProvinceTop3")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取数据
    val logs = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/data/advert/Advert.log")

    // 提取有用数据
    // 切分数据；Array（1562085629599	Hebei	Shijiazhuang	564	1）
    val logsArray: RDD[Array[String]] = logs.map(_.split("\t"))

    // 以省份进行分组
    val proGrouped: RDD[(String, Iterable[Array[String]])] = logsArray.groupBy(_(1))
    // 将分组后的组内的adid生成元组，便于以后组内聚合
    val adTup: RDD[(String, List[(String, Int)])] = proGrouped.mapValues(_.toList.map(x => (x(4), 1)))

    // 以广告id进行分组
    val grouped: RDD[(String, Map[String, List[(String, Int)]])] =
      adTup.mapValues(x => x.groupBy(_._1))

    // 分组后求出广告id对应的访问量
    val sumed: RDD[(String, Map[String, Int])] = grouped.mapValues(_.mapValues(_.size))

    // 降序排序
    val res: RDD[(String, List[(String, Int)])] =
      sumed.mapValues(_.toList.sortWith(_._2 > _._2).take(3))

    println(res.collect.toBuffer)


    sc.stop()
  }
}
