package com.qf.day14

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * DSL语言风格
  */
object SparkSQLDemo5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLDemo5").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D://data/people.json")

    import spark.implicits._
//    df.show
//    df.printSchema()
//    df.select("name").show
//    df.select($"name", $"age" + 1).show()
    df.groupBy("age").count().show() // 此时的count属于组内count
    df.filter($"age" > 20).show

    spark.stop
  }
}

