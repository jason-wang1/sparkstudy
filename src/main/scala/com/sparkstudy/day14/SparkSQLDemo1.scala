package com.sparkstudy.day14

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: RDD--->DataFrame 直接手动指定schema
  */
object SparkSQLDemo1 {
  def main(args: Array[String]): Unit = {
    //创建Core环境
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL1").setMaster("local")
    val sc = new SparkContext(conf)

    val data: RDD[Array[String]] = sc.textFile("D://data/people.txt").map(_.split(","))
    val tup: RDD[(String, String)] = data.map(line => (line(0), line(1).trim))

    //创建SparkSQL的初始化环境
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD转换为DataFrame
    //如果需要RDD于DataFrame之间操作,那么需要引用 import spark.implicits._
    //这里的spark不是包名,是SparkSession对象
    import spark.implicits._
    val df: DataFrame = tup.toDF("name", "age")
    df.show()

    sc.stop()
    spark.stop()

  }
}
