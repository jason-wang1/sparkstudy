package com.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年07月16日
  *
  * @author WangBo
  * @version 1.0
  */
case class Action(friend_id: String, actionType: String)

object Answer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Answer").setMaster("local[3]")
      .set("spark.sql.shuffle.partitions", "10")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = Seq(
      ("1001", "1007"),
      ("1002", null),
      ("1003", "1005"),
      (null, "1006")
    ).toDF("user_id", "friend_id")

    df.show()

    df.na.drop(Seq("friend_id"))
      .show()

    df.na.drop()
      .show()

  }
}
