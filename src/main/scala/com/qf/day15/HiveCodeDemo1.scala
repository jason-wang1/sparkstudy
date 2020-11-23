package com.qf.day15

import org.apache.spark.sql.SparkSession

object HiveCodeDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveCodeDemo1")
      .config("spark.sql.warehouse.dir", "d://spark-warehouse")
      .master("local[2]")
      .enableHiveSupport() // 用于启用hive
      .getOrCreate()

    import spark.implicits._

    spark.sql("create table if not exists src_1(key int, value string)")
    spark.sql("load data local inpath 'c://kv1.txt' into table src_1")
    spark.sql("select * from src_1").show()

    spark.sql("select count(1) from src_1").show()



    spark.stop()
  }
}
