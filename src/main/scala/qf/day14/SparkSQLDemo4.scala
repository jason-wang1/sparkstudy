package com.qf.day14

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD转换为DataSet
  */
object SparkSQLDemo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLDemo4").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = sc.textFile("dir/people.txt").map(_.split(","))

    import spark.implicits._
    // 将RDD转换为DataSet
    val ds: Dataset[Person] = data.map(x => {
      Person(x(0), x(1).trim.toInt)
    }).toDS()
    ds.show()

    sc.stop
    spark.stop
  }
}

