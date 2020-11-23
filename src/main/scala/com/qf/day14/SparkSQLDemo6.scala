package com.qf.day14

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SQL语句风格
  */
object SparkSQLDemo6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLDemo6").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("dir/people.json")

    // 注册为一张临时表
    df.createOrReplaceTempView("person")
    // 用sql进行查询
    val sqlDF: DataFrame = spark.sql("select * from person where age > 20 limit 10")

    sqlDF.show()
    // 主要是应用范围的区别，该方法属于一个全局表，如果访问全局表需要给全路径
    // 这种方式使用率极少
    df.createGlobalTempView("person2")
    spark.sql("select * from global_temp.person2").show()
    spark.newSession().sql("select * from global_temp.person2").show
    
    spark.stop
  }
}

