package com.dataframe.sqlpractice

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月13日
  *
  * @author WangBo
  * @version 1.0
  */
object No10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = Seq(
      (12, 34),
      (12, 56),
      (12, 78),
      (34, 56),
      (34, 12)
    ).toDF("qqa", "qqb")

//    // 找出相互关注的qq对
//    df.alias("a").join(df.alias("b"), $"a.qqa" === $"b.qqb")
//      .where($"a.qqb" === $"b.qqa")
//      .select($"a.qqa", $"a.qqb")
//      .show()

    // 找出相互关注的qq对
    val rdd1: RDD[(Int, Int)] = df.rdd.map(row => (row.getAs[Int](0), row.getAs[Int](1)))
    val rdd2: RDD[(Int, Int)] = df.rdd.map(row => (row.getAs[Int](1), row.getAs[Int](0)))
    rdd1.join(rdd2)
      .filter{case (key, (val1, val2)) => {val1 == val2}}
      .map{case (key, (val1, val2)) => (key, val1)}
      .foreach(println)

  }
}
