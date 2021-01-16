package com.sparkstudy.day14

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Descreption: RDD转换为DataSet
  */
object SparkSQLDemo4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL3").setMaster("local")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val data: RDD[Array[String]] = sc.textFile("D://data/people.txt").map(_.split(","))

    //将RDD转换为DataSet
    import spark.implicits._
    val ds: Dataset[Person] = data.map(x => {
      Person(x(0), x(1).trim.toInt)
    }).toDS()

    ds.show()

    sc.stop()
    spark.stop()
  }
}

//构建样例类，此时用于生成Schema信息
//case class Person(name: String, age: Int)