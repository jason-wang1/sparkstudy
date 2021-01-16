package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月16日
  *
  * @author WangBo
  * @version 1.0
  */
object No08 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = Seq(
      ("001", "777", 12),
      ("001", "778", 56),
      ("001", "779", 35),
      ("001", "780", 38),
      ("002", "210", 39),
      ("002", "211", 45),
      ("002", "212", 43),
      ("002", "213", 47)
      // 区组id，账号，金币
    ).toDF("dist_id", "account", "gold")

    val w: WindowSpec = Window.partitionBy($"dist_id").orderBy($"gold".desc)
    df.select($"*", rank().over(w).alias("rk"))
      .where($"rk" < 4)
      .select($"dist_id", $"account")
      .show()


  }
}
