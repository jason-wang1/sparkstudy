package com.sparkstudy.day20

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption:
  * 1. 统计每个用户充值总金额并降序排序（10分）
  * 2. 统计所有系统类型登录总次数并降序排序（10分）
  * 3. 统计所有用户在各省登录的次数的Top3（20分）
  *
  * Date: 2019年07月20日
  *
  * @author 王 博
  * @version 1.0
  */
object Test02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL3").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D://data/JsonTest02.json")

    //注册为一张临时表
    df.createOrReplaceTempView("Logs")

    //用SQL查询
    val splited: DataFrame = spark.sql("select phoneNum, terminal, province, money, status from Logs")
    splited.show()
    splited.createOrReplaceTempView("Filter")

    //1. 统计每个用户充值总金额并降序排序（10分）
    println("1. 统计每个用户充值总金额并降序排序")
    val q1: DataFrame = spark.sql("select phoneNum, sum(money) as sum_money from filter group by phoneNum order by sum_money desc")
    q1.show()

    //2. 统计所有系统类型登录总次数并降序排序（10分）
    println("2. 统计所有系统类型登录总次数并降序排序")
    val q2: DataFrame = spark.sql("select terminal, count(*) as count from filter group by terminal order by count desc")
    q2.show()

    //3. 统计所有用户在各省登录的次数的Top3（20分）
    println("3. 统计所有用户在各省登录的次数的Top3")
    val q3_1: DataFrame = spark.sql("select phoneNum, province, count(*) as cnt from filter group by province, phoneNum")
    q3_1.createOrReplaceTempView("q3_1")
    val q3_2: DataFrame = spark.sql("select phoneNum, province, cnt, row_number() over(distribute by province sort by cnt desc) as r from q3_1")
    q3_2.createOrReplaceTempView("q3_2")
    val q3: DataFrame = spark.sql("select phoneNum, province, cnt from q3_2 where r < 4")
    q3.show()


    spark.stop()
  }

}
