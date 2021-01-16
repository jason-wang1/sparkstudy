package com.sparkstudy

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Descreption: 二次排序
  * Date: 2019年08月14日
  *
  * @author WangBo
  * @version 1.0
  */
object Sort2 {
  def main(args: Array[String]): Unit = {
    //创建Core环境
    val conf: SparkConf = new SparkConf().setAppName("Sort2").setMaster("local")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.textFile("D://data/text.txt")
    text.collect().toList.foreach(println)

    val tups: RDD[(String, Int)] = text.flatMap(_.split(" ")).map((_, 1))

    val sumed: RDD[(String, Int)] = tups.reduceByKey(_+_)
    val valueGrouped: RDD[(Int, Iterable[(String, Int)])] = sumed.groupBy(_._2)

    //按单词字典升序排序
    val sortedBySreing: RDD[(Int, List[(String, Int)])] = valueGrouped.map {
      item =>
        (item._1, item._2.toList.sortWith(_._1 < _._1))
    }

    //按单词数量降序排序
    val sorted: RDD[(Int, List[(String, Int)])] = sortedBySreing.sortByKey(false)

    val res: RDD[(String, Int)] = sorted.flatMap(_._2)
    res.collect().toList.foreach(println)

  }

}
