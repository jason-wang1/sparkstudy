package com.dataframe.sqlpractice

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月10日
  *
  * @author WangBo
  * @version 1.0
  */
object No01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
      .set("spark.sql.shuffle.partitions", "10")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
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
    ).toDF("uid", "subject_id", "score")

    df.repartition(col("subject_id"))
      .coalesce(1).write.csv("D:\\temp\\df.txt")

    // 找出所有科目成绩都大于某一学科平均乘积的学生
    val w: WindowSpec = Window.partitionBy($"subject_id")
    df
      .select($"uid", $"subject_id", $"score", avg($"score").over(w).alias("avg_score"))
      .where($"score" > $"avg_score")
      .groupBy($"uid").agg(count($"uid").alias("count"))
      .where($"count" === 3)
      .show()


    df.createOrReplaceTempView("student")
    spark
      .sql("select uid, subject_id from student where score > 85")
      .explain(true)

    df.createOrReplaceTempView("t")
    spark.sql(
      """
        |select uid
        |from(
        | select uid,
        | case when score > avg_score then 1 else 0 end flag
        | from(
        |  select uid, score, avg(score) over(partition by subject_id) avg_score
        |  from t))
        |group by uid
        |having sum(flag) = 3
      """.stripMargin).show()

  }
}
