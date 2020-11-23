package com.sparkstudy.day14

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月12日
  *
  * @author WangBo
  * @version 1.0
  */
object SparkSessionDemo01 {
  def main(args: Array[String]): Unit = {
    //第三种方式
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkSessionDemo")
      .master("local")
      .enableHiveSupport()  //用于启用hive支持
      .getOrCreate()

    spark.read.json("D://data/people.json").show()

    spark.stop()
  }

}
