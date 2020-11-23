package com.sparkstudy.day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: 求每个学科各个模块访问量再取top3
  * 20161123101523	http://java.learn.com/java/javaee.shtml
  * 时间            http://学科/学科/模块
  * 结果示例：
  * (ui.learn.com,List((ui.learn.com,http://ui.learn.com/ui/video.shtml,37), (ui.learn.com,http://ui.learn.com/ui/course.shtml,26), (ui.learn.com,http://ui.learn.com/ui/teacher.shtml,23))),
  * 数据：D:\千锋\课堂记录\Spark\Day10\周考\access.txt
  * Date: 2019年07月06日
  * @author 王博
  * @version 1.0
  */
object WeekTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("weektest").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    //获取数据
    val accessLog: RDD[String] = sc.textFile("D://data/access.txt")
    val url: RDD[String] = accessLog.map(_.split("\t")(1))

    // RDD[(学科，模块)]
    val subMod: RDD[(String, String)] = url.map(x => (x.split("/")(2), x))

    //按学科分组；RDD[学科，Iterable[(学科，模块)]]
    val subGrouped: RDD[(String, Iterable[(String, String)])] = subMod.groupBy(_._1)

    //按模块分组；RDD[学科，Map[模块，Iterable[学科，模块]]]
    val subModGrouped: RDD[(String, Map[String, Iterable[(String, String)]])] = subGrouped.mapValues(x => x.groupBy(_._2))

    //求和，RDD[学科，Map[模块，数量]]
    val sumed: RDD[(String, Map[String, Int])] = subModGrouped.mapValues(_.mapValues(_.size))

    //排序
    val res: List[(String, List[(String, Int)])] = sumed.mapValues(_.toList.sortWith(_._2 > _._2).take(3)).collect().toList

    for (i <- res) println(i)
  }
}
