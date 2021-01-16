package com.sparkstudy.day15

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex

/**
  * 王博
  */
object CDNTest {

  //匹配IP地址
  val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  //匹配视频文件名
  val videoPattern = "([0-9]+).mp4".r

  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
  val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配 http 响应码和请求数据大小
  private val httpSizePattern: Regex = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CDNTest")
    val sc = new SparkContext(conf)

    val input: RDD[String] = sc.textFile("D://data/cdn.txt").cache()

//    input.collect().toList.take(3).foreach(println)

    // 统计独立IP访问量前10位
    ipStatics(input)

    //统计每个视频独立IP数
    videoIpStatics(input)

    // 统计一天中每个小时间的流量
    flowOfHour(input)

    sc.stop()
  }

  //统计一天中每个小时间的流量
  def flowOfHour(data: RDD[String]): Unit = {
//    val filtered: RDD[String] = data.filter(x => isMatch(httpSizePattern,x)).filter(x => isMatch(timePattern,x))

    //(访问时间,访问大小)
//    val timeSize: RDD[(String, Long)] = filtered.map(x=>getTimeAndSize(x))


  }



  // 统计每个视频独立IP数
  def videoIpStatics(data: RDD[String]): Unit = {
//    val mp4: RDD[String] = data.map(x=>videoPattern.findFirstIn(x).get)
    val mp4Logs: RDD[String] = data.filter(_.contains(".mp4"))

    //(文件名，ip)
    val tupmp4Ip: RDD[(String, Option[String])] = mp4Logs.map(x => (videoPattern.findFirstIn(x).get, IPPattern findFirstIn x))

    val grouprd: RDD[(String, Iterable[Option[String]])] = tupmp4Ip.groupByKey()

    //取除重复ip；(文件名，数量)
    val sumed: RDD[(String, Int)] = grouprd.map(x => (x._1, x._2.toList.distinct.size))

    //排序
    val sorted: Array[(String, Int)] = sumed.sortBy(_._2, false).take(10)

    //打印
    println("****************************************")
    println("统计每个视频独立IP数：（取了前10条数据展示）")
    sorted.foreach(println)

  }

  // 统计独立IP访问量前10位
  def ipStatics(data: RDD[String]): Unit = {
    val ip: RDD[String] = data.flatMap(x => IPPattern findFirstIn(x))
    val tupIp: RDD[(String, Int)] = ip.map(x => (x, 1))

    //聚合
    val sumed: RDD[(String, Int)] = tupIp.reduceByKey((x, y) => x+y)

    //排序
    val sorted: Array[(String, Int)] = sumed.sortBy(_._2, false).take(10)

    println("**********************")
    println("统计独立IP访问量前10位：")
    sorted.foreach(println)

    println("独立ip数：" + sumed.collect().length)

  }

}