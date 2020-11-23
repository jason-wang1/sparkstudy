package com.qf.day15

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 写入数据到mysql
  */
object SparkSQLAndMySQL2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLAndMySQL2")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("dir/employees.json")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    df.write.jdbc("jdbc:mysql://node03:3306/bigdata", "employees", prop)


    spark.stop()
  }
}
