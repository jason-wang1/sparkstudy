package com.qf.day09

import org.apache.spark.{SparkConf, SparkContext}

object Test09_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val accesslog = sc.textFile("D://teachingprogram/sparkcoursesinfo/spark/pvuv/access.log")

    val ip = accesslog.map(_.split(" ")(0))

    val pv = ip.map(x => ("pv", 1)).reduceByKey(_ + _)
    val pv1 = ip.count()

    val uv = ip.distinct.count

    println("pv:" + pv.collect.toBuffer)
    println("pv1:" + pv1)
    println("uv:" + uv)
  }
}
