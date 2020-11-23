package com.sparkstudy.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption:
  * 1562085629599	Hebei	Shijiazhuang	 564	  1
  * 时间戳         省份  城市          用户id  广告id
  * Date: 2019年07月06日
  *
  * @author WangBo
  * @version 1.0
  */
object Advent_ProvinceTop3_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("AdventProvinceTop3").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    //获取数据
    val logs: RDD[String] = sc.textFile("D://data/Advert.log")

    //RDD[Array[时间戳, 省份, 城市, 用户id, 广告id]]
    val logsArray: RDD[Array[String]] = logs.map(_.split("\t"))

    //以省份分组；RDD[省份，Iterable[Array[时间戳, 省份, 城市, 用户id, 广告id]]]
    val proGrouped: RDD[(String, Iterable[Array[String]])] = logsArray.groupBy(_(1))

    //提取所需的字段；RDD[省份，List[广告id，1]]
    val adTup: RDD[(String, List[(String, Int)])] = proGrouped.mapValues(_.toList.map(x => (x(4), 1)))

    //以广告id分组；RDD[省份，Map[广告id，List[广告id，1]]]
    val proIdGrouped: RDD[(String, Map[String, List[(String, Int)]])] = adTup.mapValues(_.groupBy(_._1))

    //求和；RDD[省份，Map[广告id，数量]]
    val sumed: RDD[(String, Map[String, Int])] = proIdGrouped.mapValues(_.mapValues(_.size))

    //排序
    val res: List[(String, List[(String, Int)])] = sumed.mapValues(_.toList.sortWith(_._2 > _._2).take(3)).collect().toList

    println(res)

    sc.stop()

  }

}
