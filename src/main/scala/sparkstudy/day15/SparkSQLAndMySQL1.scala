package com.sparkstudy.day15

import java.util.Properties

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月18日
  *
  * @author WangBo
  * @version 1.0
  */
object SparkSQLAndMySQL1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkSQLAndMySQL1")
      .master("local[2]")
      .getOrCreate()

//    //方式一
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")
//    val url = "jdbc:mysql://NODE01:3306/test"
//    val df: DataFrame = spark.read.jdbc(url, "dept", prop)

    //方式二
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://NODE01:3306/test")
      .option("dbtable", "dept")
      .option("user", "root")
      .option("password", "root")
      .load()

    val columnStrs: Array[String] = df.columns
    val columns = new ListBuffer[Column]()
    for (elem <- columnStrs) {
      val column =
      if (elem.equals("...")) {
        df.col(elem).cast("String")
      } else
        df.col(elem)
      columns.append(column)
    }

    val frame: DataFrame = df.select(columns:_*)

    frame.withColumn("Date", frame.col("Date").cast(StringType))

    df.show()

    spark.stop()
  }

}
