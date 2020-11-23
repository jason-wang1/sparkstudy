package com.qf.day14

import org.apache.spark.sql.SparkSession

/**
  * 用udf实现字符串拼接
  */
object UDFDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDFDemo1")
      .master("local")
      .getOrCreate()

    val df = spark.read.json("D://data/people.json")

    // 注册函数，注册后正在整个应用中都可以用
    spark.udf.register("newname", (x: String) => "name:" + x)

    df.createOrReplaceTempView("person")
    spark.sql("select newname(name), age, facevalue from person").show()

    spark.stop()
  }
}
