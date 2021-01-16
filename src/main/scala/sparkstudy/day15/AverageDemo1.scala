package com.sparkstudy.day15

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月13日
  *
  * @author WangBo
  * @version 1.0
  */
object AverageDemo1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("AverageDemo1").master("local").getOrCreate()

    //获取数据
    val df: DataFrame = spark.read.json("D://data/Score.json")
    df.show()

    //需求：统计每个班级最高成绩的信息   --》（分组并排序）
    //建表
    df.createOrReplaceTempView("t_score")

    //开窗实现需求
    spark.sql("select * from " +
      "(select name, class, score, rank() over(distribute by class sort by score desc) as rank from t_score) " +
      "as t where t.rank = 1")
      .show()

    spark.stop()
  }
}
