package com.dataframe.sqlpractice2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年07月02日
  *
  * @author WangBo
  * @version 1.0
  */
object No02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No02").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val frame: DataFrame = Seq(
      (1, "1", 23),
      (2, "1", 12),
      (3, "1", 12),
      (4, "1", 32),
      (5, "1", 342),
      (6, "2", 13),
      (7, "2", 34),
      (8, "2", 13),
      (9, "2", 134)
    ).toDF("uid", "channel", "min")

//    frame.groupBy($"channel").agg(count($"min"), sum($"min")).show(true)

    frame.createOrReplaceTempView("vedio")
    spark.sql(
      """
        |select channel, count(min), sum(min)
        |from vedio
        |group by channel
      """.stripMargin).show()
  }
}
