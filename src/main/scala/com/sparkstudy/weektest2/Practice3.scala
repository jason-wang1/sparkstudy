package com.sparkstudy.weektest2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年08月14日
  *
  * @author WangBo
  * @version 1.0
  */
object Practice3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Practice2").setMaster("local")
    val sc = new SparkContext(conf)

    //(uid, did)
    val t1: RDD[(String, String)] = sc.textFile("D://data/t1.txt")
      .map(_.split("\t"))
      .map { item =>
        (item(0), item(1))
      }


    //(did, 1)
    val t2: RDD[(String, Int)] = sc.textFile("D://data/t2.txt")
      .map(_.split("\t"))
      .map { item =>
        (item(0), 1)
      }

    val sumed_did: RDD[(String, Int)] = t2.reduceByKey(_+_)
    sumed_did.collect().toList.foreach(println)

    val unit: RDD[(String, (String, Option[Int]))] = t1.leftOuterJoin(sumed_did)

    unit.collect().toList.foreach(println)
  }

}
