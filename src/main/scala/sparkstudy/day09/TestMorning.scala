package com.sparkstudy.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Descreption: 求网站访问的PV、UV
  * Date: 2019年07月05日
  *
  * @author WangBo
  * @version 1.0
  */
object TestMorning {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val conf: SparkConf = new SparkConf()
    conf.setAppName("WebsiteAccess").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    //计算指标
    val accesslog: RDD[String] = sc.textFile("D://data/access.log")
    val ip: RDD[String] = accesslog.map(_.split(" ")(0))
//    println(ip.collect().toBuffer)
    val pv: RDD[(String, Int)] = ip.map(x => ("pv", 1)).reduceByKey(_ + _)
    pv.foreach(println(_))
    val pv1 = ip.count()
    println(pv1)
    val uv = ip.distinct().count()
    println(uv)


  }

}
