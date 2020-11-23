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
object Test8_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10)

    val sample: RDD[Int] = rdd.sample(true, 0.3)

    val result = sample.collect()toBuffer

    println(result)
  }

}
