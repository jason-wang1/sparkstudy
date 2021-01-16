package com.review.mycode

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年11月13日
  *
  * @author WangBo
  * @version 1.0
  */
object coalesceDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName(s"${this.getClass.getCanonicalName}")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6), 3)
    val rdd2 = rdd1.map((_, 1))
    val rdd3 = rdd2.map((_, 2)).coalesce(2, false)

//    rdd2.join(rdd3, 3)
    rdd3.foreach(println)

    Thread.sleep(1000000)
    sc.stop()
  }
}
