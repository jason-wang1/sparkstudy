package com.qf.day14

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过反射的方式构建Schema
  */
object SparkSQLDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLDemo2").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("dir/people.txt").map(_.split(","))
    // 用样例类的方式将数据进行类型映射
    val tupRDD: RDD[Person] = data.map(x => Person(x(0), x(1).trim.toInt))

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val df: DataFrame = tupRDD.toDF() // 直接调用toDF方法，不用传参指定Schema
    df.show()
    val rdd: RDD[Row] = df.rdd

    sc.stop()
    spark.stop()
  }
}
// 构建样例类，此时用于生成Schema信息
case class Person(name: String, age: Int)