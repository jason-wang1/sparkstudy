package com.qf.day14

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD转换为DataFrame
  */
object SparkSQLDemo1 {
  def main(args: Array[String]): Unit = {
    // 创建Core的环境
    val conf = new SparkConf().setAppName("SparkSQLDemo1").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("dir/people.txt").map(_.split(","))
    val tup: RDD[(String, Int)] = data.map(x => (x(0), x(1).trim.toInt))

    // 创建SparkSQL的初始化环境
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // RDD转换为DataFrame
    import spark.implicits._
    val df: DataFrame = tup.toDF("name", "age") // 相当于调用toDF方法的同时指定了schema
    df.show()

    sc.stop()
    spark.stop()
  }
}
