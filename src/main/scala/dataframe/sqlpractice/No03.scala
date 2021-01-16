package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月10日
  *
  * @author WangBo
  * @version 1.0
  */
object No03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df: DataFrame = Seq(
      ("u1", "a"),
      ("u2", "b"),
      ("u2", "b"),
      ("u1", "b"),
      ("u1", "b"),
      ("u1", "a"),
      ("u3", "c"),
      ("u4", "b"),
      ("u1", "a"),
      ("u2", "c"),
      ("u5", "b"),
      ("u4", "b"),
      ("u6", "c"),
      ("u2", "c"),
      ("u1", "b"),
      ("u2", "a"),
      ("u2", "a"),
      ("u2", "a"),
      ("u2", "a")
    ).toDF("userId", "shopId")

    // 每个店铺的 UV（访客数）
//    df
//      .distinct()
//      .groupBy($"userId").agg(count($"shopId"))
//      .show()

    // 每个店铺top3访客信息，输出：店铺名称、访客id，访问次数

    val w: WindowSpec = Window.partitionBy($"shopId").orderBy($"visitCount".desc)
    df
      .groupBy($"userId", $"shopId").agg(count($"*").alias("visitCount"))
      .select($"*", rank().over(w).alias("rk"))
      .where($"rk" < 4)
      .show()

  }
}
