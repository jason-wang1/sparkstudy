package com.qf.day15

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 获取mysql的数据
  */
object SparkSQLAndMySQL1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLAndMySQL1")
      .master("local[2]")
      .getOrCreate()

    // 方式一
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")
//    val url = "jdbc:mysql://node03:3306/bigdata"
//
//    val df: DataFrame = spark.read.jdbc(url, "person", prop)

    // 方式二
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://node01:3306/bigdata")  //bigdata是数据库名
      .option("dbtable", "person")  //person是表名
      .option("user", "root")
      .option("password", "root")
      .load()

    df.show()

    spark.stop()
  }
}
