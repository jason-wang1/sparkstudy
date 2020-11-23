package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月14日
  *
  * @author WangBo
  * @version 1.0
  */
object No06 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = Seq(
      ("123", 39, "2019-02-11", "001"),
      ("127", 76, "2019-03-11", "002"),
      ("123", 87, "2019-10-02", "003"),
      ("129", 87, "2019-10-03", "004"),
      ("123", 65, "2019-10-16", "005")
    ).toDF("userId", "money", "paymentTime", "orderId")

    // 计算所有用户中在今年10月第一次购买商品的金额
    val w: WindowSpec = Window.partitionBy($"userId").orderBy($"paymentTime", $"orderId")
    df.withColumn("month", $"paymentTime".substr(6, 2))
      .where($"month" === "10")
      .select($"userId", $"money", rank().over(w).alias("rk"))
      .where($"rk" === 1)
      .drop($"rk")
      .show()
  }
}
