package com.qf.day14

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 创建SparkSession的几种方式
  */
object SaprkSessionDemo1 {
  def main(args: Array[String]): Unit = {

    // 第一种方式
//    val spark: SparkSession = SparkSession
//      .builder() // 构建SparkSession对象
//      .appName("SaprkSessionDemo1") // 配置app的名称
//      .master("local") // 配置运行模式
//      .getOrCreate() // 开始创建初始化对象

    // 第二种方式
//    val conf = new SparkConf()
//      .setAppName("SaprkSessionDemo1")
//      .setMaster("local")
//    val spark = SparkSession
//      .builder()
//      .config(conf)
//      .getOrCreate()

    // 第三种方式
    val spark = SparkSession
      .builder()
      .appName("SaprkSessionDemo1")
      .master("local")
      .enableHiveSupport() // 用于启用hive支持
      .getOrCreate()

    spark.read.json("c://people.json").show()

    spark.stop()
  }
}
