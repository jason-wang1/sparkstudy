package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月16日
  *
  * @author WangBo
  * @version 1.0
  */
object No12 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = Seq(
      ("1-1", -10),
      ("1-2", null),
      ("1-3", null),
      ("1-4", -50),
      ("1-5", null),
      ("1-6", null),
      ("1-7", null),
      ("1-8", null)
      // 日期，金额
    ).toDF("date", "money")

    // 把金额为null的数据替换为前面的值

  }
}
