package com.qf.day14

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过StructType的方式生成Schema
  */
object SparkSQLDemo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLDemo3").setMaster("local")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = sc.textFile("dir/people.txt").map(_.split(","))

    // 生成Schema信息
    val schema: StructType = StructType(
      Array(
        StructField("name", StringType, false),
        StructField("age", IntegerType, true)
      )
    )

    // 将RDD映射到RowRDD上
    val rowRDD: RDD[Row] = data.map(x => Row(x(0), x(1).trim.toInt))

    // 将schema信息和rowRDD进行对应并封装到DataFrame里
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)
    df.show()
    val rdd: RDD[Row] = df.rdd

    sc.stop
    spark.stop
  }
}
