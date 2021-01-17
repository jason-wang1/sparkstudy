package com.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import ulits.SparkConfig

/**
  * 输入：schema为（学生id, 科目id, 分数）
  * 输出：schema为（学生id）
  *
  * 输出所有科目成绩都大于某一学科平均乘积的学生
  */
object Df01 {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConfig("Df01").getSparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df: DataFrame = Seq(
      ("1001", "01", 90),
      ("1001", "02", 90),
      ("1001", "03", 90),
      ("1002", "01", 85),
      ("1002", "02", 85),
      ("1002", "03", 70),
      ("1003", "01", 70),
      ("1003", "02", 70),
      ("1003", "04", 70),
      ("1003", "03", 85)
    ).toDF("uid", "subjectId", "score")

    // 找出所有科目成绩都大于某一学科平均乘积的学生
    val w: WindowSpec = Window.partitionBy($"subjectId")
    df
      .withColumn("avgScore", avg($"score").over(w))
      .where($"score" > $"avgScore")
      .groupBy($"uid").agg(count($"uid").alias("count"))
      .where($"count" === 3)
      .show()
  }
}
