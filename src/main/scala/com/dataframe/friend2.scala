package com.dataframe

import java.util

import org.apache.spark.sql.functions.collect_list
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * Descreption: 计算二度好友
  * Date: 2020年04月13日
  *
  * @author WangBo
  * @version 1.0
  */
object friend2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DfDemo").setMaster("local[3]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val frindsDF: DataFrame = Seq[(String, String)](("a", "b"), ("a", "c"), ("a", "e"), ("b", "d"), ("e", "d"), ("c", "f"), ("f", "g"))
      .toDF("col1", "col2").cache()

//    solve1(frindsDF, spark)

    solve2(frindsDF, spark)

  }


  def solve2(frindsDF: DataFrame, spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val unionFriendDF: Dataset[Row] = frindsDF
      .union(frindsDF.select($"col2", $"col1"))

    unionFriendDF.show()
    unionFriendDF.alias("t1")
      .join(unionFriendDF.alias("t2"), "col1")
      .where($"t1.col2" < $"t2.col2")
      .select($"t1.col2".alias("col1"), $"t2.col2".alias("col2"))
      .except(unionFriendDF)
      .show()
  }

  def solve1(frindsDF: DataFrame, spark: SparkSession) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val friendsAllDF: Dataset[Row] = frindsDF
      .union(frindsDF.selectExpr("col2 as col1", "col1 as col2"))
    val groupedDF: DataFrame = friendsAllDF
      .groupBy($"col1").agg(collect_list($"col2").alias("friends"))
    groupedDF.show()
    groupedDF
      .rdd
      .flatMap(row => {
        val arr: util.List[String] = row.getList(1)
        val friends2: mutable.ListBuffer[(String, String)] = mutable.ListBuffer[(String, String)]()
        for (i <- 0 until arr.size()-1) {
          for (j <- i+1 until arr.size()) {
            friends2.append((arr.get(i), arr.get(j)))
          }
        }
        friends2
      })
      .distinct()
      .toDF("col1", "col2")
      .except(friendsAllDF)
      .show()
  }
}
