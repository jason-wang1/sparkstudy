package com.sparkstudy.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */
object BroadcastDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("BroadcastDemo").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    //该变量的值在Driver端
    val list = List("favicon", "admin")

    val lines: RDD[String] = sc.textFile("D://data/access.log")
    val splited: RDD[Array[String]] = lines.map(_.split(" "))
    val filtered: RDD[Array[String]] = splited.filter(list.contains(_))

    filtered.map(_.toList).foreach(println)


    

  }

}
