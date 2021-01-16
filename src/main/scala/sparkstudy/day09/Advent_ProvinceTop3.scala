package com.sparkstudy.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Descreption:
  * 1562085629599	Hebei	Shijiazhuang	 564	  1
  * 时间戳         省份  城市          用户id  广告id
  * Date: 2019年07月05日
  *
  * @author WangBo
  * @version 1.0
  *
  * 需求一：统计每个省份广告点击top3
  */
object Advent_ProvinceTop3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Advent_ProvinceTop3").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //获取数据
    val logs: RDD[String] = sc.textFile("D://data/Advert.log")
    val mid1: mutable.Buffer[String] = logs.collect().toBuffer
//    println(mid1)

    //提取有用数据
    //切分数据；Array（1562085629599, Hebei, Shijiazhuang, 564, 1）
    val logsArray: RDD[Array[String]] = logs.map(_.split("\t"))
    val length = logsArray.collect().length
//    println(length)
    //以省份分组；（Henan，Iterator[Array(1562085684117, Henan, Zhengzhou, 745, 3), Array(...), ...]）...
    val proGrouped: RDD[(String, Iterable[Array[String]])] = logsArray.groupBy(_(1))
    //    val mid3: List[Iterable[Array[String]]] = proGrouped.collect().toList.map(_._2)
    //    val mid3_1: Iterable[Array[String]] = mid3(1)
    //    for (i <- mid3_1) println

    //提取字段，转换粒度：（province, (adid, 1)...） （henan, List[(3, 1), (x, 1), ...]）...
    val adTup: RDD[(String, List[(String, Int)])] = proGrouped.mapValues(_.toList.map(x => (x(4), 1)))

    //以广告id进行分组；（henan, Map[(3, List[(3, 1), (3, 1), ...), ...]]）...
    val grouped: RDD[(String, Map[String, List[(String, Int)]])] = adTup.mapValues(x => x.groupBy(_._1))

    //分组后求出广告id对应的访问量（henan, Map[(3, 70), ...]）...
    val sumed: RDD[(String, Map[String, Int])] = grouped.mapValues(_.mapValues(_.size))

    //降序排序并取top3
    val res: RDD[(String, List[(String, Int)])] = sumed.mapValues(_.toList.sortWith(_._2 > _._2).take(3))

    println(res.collect().toBuffer)

    sc.stop()

  }

}
