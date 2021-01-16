package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月12日
  *
  * @author WangBo
  * @version 1.0
  */
object No05 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = Seq(
      ("2019-02-11", "test_1", 23),
      ("2019-02-11", "test_2", 19),
      ("2019-02-11", "test_3", 39),
      ("2019-02-11", "test_1", 23),
      ("2019-02-11", "test_3", 39),
      ("2019-02-11", "test_1", 23),
      ("2019-02-12", "test_2", 19),
      ("2019-02-13", "test_1", 23),
      ("2019-02-15", "test_2", 19),
      ("2019-02-16", "test_2", 19)
    ).toDF("date", "userId", "age")

    // 求所有用户的总数及平均年龄
//    df.selectExpr("userId", "age", "1 as key").distinct()
//      .groupBy($"key").agg(count($"userId").alias("userNum"), avg($"age").alias("avgAge"))
//      .drop($"key")
//      .show()

    // 求活跃用户总数及其平均年龄
    val w: WindowSpec = Window.partitionBy($"userId").orderBy($"date")
    df.distinct().select($"date", $"userId", $"age", rank().over(w).alias("rk"))
      .withColumn("diff", dayofyear($"date") - $"rk")
      .groupBy($"userId", $"diff").agg(count($"userId").alias("userNum"), min($"age").alias("age"))
      .where($"userNum" > 1).selectExpr("userId", "age", "1 as key").distinct().groupBy($"key").agg(count("userId"), avg("age"))
      .show()

  }
}
