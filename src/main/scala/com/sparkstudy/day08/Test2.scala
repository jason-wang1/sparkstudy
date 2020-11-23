package com.sparkstudy.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月03日
  *
  * @author WangBo
  * @version 1.0
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val conf: SparkConf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)

  }

}
