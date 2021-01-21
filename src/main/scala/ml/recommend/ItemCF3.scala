package com.ml.recommend

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Descreption:
  * 最佳方案：不采用任何数据结构(哈希表，有序数组)表示物品的评分向量，
  * 而是直接让(userId, itemId, score)评分表与自己join。
  * 在笛卡尔积时，如果两边表的userId匹配不上，直接就删除掉，因此笛卡尔积后的表相对来说会很小
  *
  * Date: 2020年11月23日
  *
  * @author WangBo
  * @version 1.0
  */
object ItemCF3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ItemCf3")
      .setMaster("local[*]")

    val spark = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = Seq(
      ("Bob", "A", 5.0),
      ("Bob", "D", 1.0),
      ("Bob", "E", 1.0),
      ("Ali", "C", 1.0),
      ("Ali", "E", 4.0),
      ("Tom", "A", 4.0),
      ("Tom", "B", 2.0),
      ("Tom", "E", 2.0),
      ("Zoe", "A", 2.0),
      ("Zoe", "B", 5.0),
      ("Amy", "B", 1.0),
      ("Amy", "C", 4.0),
      ("Amy", "D", 4.0)
    ).toDF("userId", "itemId", "score")

    val itemLenDF: DataFrame = df
      .groupBy($"itemId")
      .agg(sqrt(sum($"score" * $"score")).alias("vecLen"))
      .cache()
    itemLenDF.show(false)

    val item2itemDF: DataFrame = df.alias("t1")
      .join(df.alias("t2"),
        $"t1.userId" === $"t2.userId" && $"t1.itemId" =!= $"t2.itemId",
        "inner")
      .selectExpr("t1.itemId as itemId1", "t2.itemId as itemId2", "t1.score * t2.score as mul")
      .groupBy($"itemId1", $"itemId2")
      .agg(sum($"mul").alias("dot"))
      .cache()
    item2itemDF.show(false)

    val itemSimDF: DataFrame = item2itemDF
      .join(itemLenDF.alias("item1"), $"itemId1" === $"item1.itemId")
      .join(itemLenDF.alias("item2"), $"itemId2" === $"item2.itemId")
      .withColumn("cosSim", $"dot" / ($"item1.vecLen" * $"item2.vecLen"))
    itemSimDF.show(false)
  }
}
