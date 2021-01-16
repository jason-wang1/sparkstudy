package com.sparkstudy.day14

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Descreption: SQL语言风格
  */
object SparkSQLDemo6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL3").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D://data/people.json")

    //注册为一张临时表
    df.createOrReplaceTempView("person")

    //用SQL查询
    val sqlDF: DataFrame = spark.sql("select * from person where age > 20 limit 10")
    sqlDF.show()

    //该方法属于一个全局表，如果访问全局表需要给全路径，用的很少
    df.createOrReplaceGlobalTempView("person2")
    spark.sql("select * from global_temp.person2").show()
    spark.newSession().sql("select * from global_temp.person2").show()

    spark.stop()
  }
}

