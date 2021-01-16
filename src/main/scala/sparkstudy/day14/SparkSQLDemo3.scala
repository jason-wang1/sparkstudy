package com.sparkstudy.day14

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: 通过StructType的方式生成Schema。最常见的方式
  * Date: 2019年07月12日
  *
  * @author WangBo
  * @version 1.0
  */
object SparkSQLDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL3").setMaster("local")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val data: RDD[Array[String]] = sc.textFile("D://data/people.txt").map(_.split(","))

    //指定Schema信息
    val schema = StructType(
      Array(
        StructField("name", StringType, false),
        StructField("age", IntegerType, true)
      )
    )
    schema

    //将RDD映射到RowRDD上
    val rowRDD: RDD[Row] = data.map(x => Row(x(0), x(1).trim.toInt))

    //将schema信息和rowRDD进行对应并封装到DataFrame里
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)
    df.show()

    sc.stop()
    spark.stop()
  }

}
