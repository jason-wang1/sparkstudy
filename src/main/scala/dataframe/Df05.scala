package com.dataframe

import java.util
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ulits.SparkConfig

import scala.collection.mutable

/**
  * 输入：schema为（用户A，用户B）
  * 输出：schema为（用户A，用户B）
  *
  * 给一张好友表，输出二度好友表
  */
object Df05 {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConfig("Df05").getSparkSession
    import spark.implicits._
    val frindsDF: DataFrame = Seq[(String, String)](
      ("a", "b"),
      ("a", "c"),
      ("a", "e"),
      ("b", "d"),
      ("e", "d"),
      ("c", "f"),
      ("f", "g")
    )
      .toDF("col1", "col2").cache()

    solveWithDF(frindsDF, spark)

    //    solveWithRDD(frindsDF, spark)
  }


  def solveWithDF(frindsDF: DataFrame, spark: SparkSession): Unit = {
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

  def solveWithRDD(frindsDF: DataFrame, spark: SparkSession) = {
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
