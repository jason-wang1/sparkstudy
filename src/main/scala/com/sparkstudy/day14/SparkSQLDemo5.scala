package com.sparkstudy.day14

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Descreption: DSL语言风格
  */
object SparkSQLDemo5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQLDemo5").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D://data/people.json")
    df.show()

    import spark.implicits._
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age"+1).show()
    df.groupBy("age").count().show()
    df.filter($"age">20).show()

    spark.stop()
  }
}

