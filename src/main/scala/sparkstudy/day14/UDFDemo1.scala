package com.sparkstudy.day14

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: 用udf实现字符串拼接
  */
object UDFDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDFDemo1")
      .master("local")
      .getOrCreate()

    val df: DataFrame = spark.read.json("D://data/people.json")

    //注册函数，注册后正在整个应用中都可以用
    spark.udf.register("newname", (x: String) => "name:"+x)

    df.show()
    df.createOrReplaceTempView("person")
    spark.sql("select newname(name), name, age from person").show()

    spark.stop()
  }
}
