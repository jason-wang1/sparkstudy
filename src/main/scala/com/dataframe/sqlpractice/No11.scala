package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月13日
  *
  * @author WangBo
  * @version 1.0
  */
object No11 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = Seq(
      ("1582044689", "001", "310"),
      ("1582044692", "002", "310"),
      ("1582044692", "002", "310"),
      ("1582044692", "002", "332"),
      ("1582044692", "002", "332"),
      ("1582044695", "001", "356"),
      ("1582044695", "001", "356"),
      ("1582044695", "001", "350")
      // 时间戳，用户id，商品id
    ).toDF("ts", "uid", "pid")



    // 求pv最大的商品
//    df.groupBy($"pid").agg(count($"uid")).show()

    // 求pv大于100的top3
    df.groupBy($"pid").agg(count($"uid").alias("pv"))
      .where($"pv" > 100)
      .sort($"pv".desc)
      .limit(3)
      .show()

  }
}
