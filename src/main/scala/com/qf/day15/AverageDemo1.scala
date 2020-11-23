package com.qf.day15

import org.apache.spark.sql.{SaveMode, SparkSession}

object AverageDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AverageDemo1").master("local").getOrCreate()
    import spark.implicits._

    val df = spark.read.json("D://data/Score.json")

    df.createOrReplaceTempView("t_score")

    df.show
    // 需求：统计每个班级最高成绩的信息   --》（分组并排序）
    spark.sql("select class, max(score) as max from t_score group by class").show

    spark.sql("select a.name, b.class, b.max from t_score as a, " +
      "(select class, max(score) as max from t_score group by class) as b " +
      "where a.score = b.max")
      .show()

    // 开窗实现需求
    spark.sql("select * from " +
      "(select name, class, score, rank() over(partition by class order by score desc) rank from t_score) " +
      "as t where t.rank = 1")

    spark.sql("select name,class,score,row_number() over(partition by class order by score desc) rank from t_score").show()


    df.write.mode(SaveMode.Append).save("d://res")

    spark.stop()
  }
}
