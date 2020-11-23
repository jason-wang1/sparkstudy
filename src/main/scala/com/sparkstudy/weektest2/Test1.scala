package com.sparkstudy.weektest2

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年08月10日
  *
  * @author WangBo
  * @version 1.0
  */
object Test1 {
  def main(args: Array[String]): Unit = {
    //创建Core环境
    val conf: SparkConf = new SparkConf().setAppName("Test1").setMaster("local")
    val sc = new SparkContext(conf)

    val baseStationsURL: URL = this.getClass.getResource("/data/base_stations.txt")
    val logs: RDD[String] = sc.textFile(baseStationsURL.getPath)

    //(1)、每个人(手机号)在每个基站停留的总时长？
    val userSiteInfo: RDD[((String, String), Long)] = logs.map(line => {
      val fields: Array[String] = line.split(",")
      val phone: String = fields(0)
      val dt: Long = fields(1).toLong
      val site: String = fields(2)
      val eventType: String = fields(3)
      val time_long = if (eventType.equals("1")) -dt else dt

      ((phone, site), time_long)
    })

    val userSiteSumTime: RDD[((String, String), Long)] = userSiteInfo.reduceByKey(_+_)

    println("(1)、每个人(手机号)在每个基站停留的总时长: ")
    userSiteSumTime.collect().toList.foreach(println)


    //(2)、每天每个基站的累计停留总时长的TOP3(每个人累计之和)?
    val userSumTime: RDD[(String, Long)] = userSiteInfo.map(x => {
      val phone: String = x._1._1
      val time_long: Long = x._2

      (phone, time_long)
    }).reduceByKey(_ + _)

    println()
    println("(2)、每天每个基站的累计停留总时长的TOP3(每个人累计之和)")
    userSumTime.collect().toList.foreach(println)


  }

}
