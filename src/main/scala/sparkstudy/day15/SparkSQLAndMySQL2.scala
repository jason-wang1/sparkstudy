package com.sparkstudy.day15

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月18日
  *
  * @author WangBo
  * @version 1.0
  */
object SparkSQLAndMySQL2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkSQLAndMySQL2")
      .master("local[2]")
      .getOrCreate()

    val df: DataFrame = spark.read.json("D://data/employees.json")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    df.write.jdbc("jdbc:mysql://node01:3306/test", "employees", prop)

    spark.stop()
  }

}
