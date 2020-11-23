package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月11日
  *
  * @author WangBo
  * @version 1.0
  */
object No04 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val frame: DataFrame = Seq(
      ("2017-01-01", "10029028", "1000003251", 33.57),
      ("2017-01-01", "10029029", "1000003255", 57.67),
      ("2017-01-01", "10029030", "1000003255", 25.76),
      ("2017-11-01", "10029031", "1000003255", 57.67),
      ("2017-11-01", "10029032", "1000003257", 32.3)
    ).toDF("date", "orderId", "userId", "amount")

    // 给出2017年每个月的订单数、用户数、总成交金额
    frame
      .withColumn("month", $"date".substr(0, 7))
      .groupBy($"month").agg(count($"orderId"), countDistinct($"userId"), sum($"amount"))
      .show()

    // 给出2017年11月新用户数（11月才有第一笔订单）
    frame
      .withColumn("month", $"date".substr(0, 7))
      .groupBy($"userId").agg(min($"month").alias("month"))
      .where($"month" === "2017-11")
      .show()

  }

}
