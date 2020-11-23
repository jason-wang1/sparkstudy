package com.sparkstudy.day15

import org.apache.spark.sql.SparkSession

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月18日
  *
  * @author WangBo
  * @version 1.0
  */
object HiveCodeDemo2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("HiveCodeDemo2")
      .config("spark.sql.warehouse.dir", "hdfs://NODE01:9000/spark-warehouse")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql("create table if not exists src_1(key int, value string)")
    spark.sql("load data local inpath '/data/kv1.txt' into table src_1")
    spark.sql("select * from src_1").show()

    spark.sql("select count(1) from src_1").show()
  }

}
