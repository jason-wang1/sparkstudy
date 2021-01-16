package com

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Descreption: XXXX<br/>
  * Date: 2020年07月01日
  *
  * @author WangBo
  * @version 1.0
  */
object HexParse {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HexParse").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._


  }
}
