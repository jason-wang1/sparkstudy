package com.release

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月31日
  *
  * @author WangBo
  * @version 1.0
  */
object DMPBusinessArea {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DMPLabel").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val logs: DataFrame = spark.read.parquet(Constant.PARQUET_PATH)
    logs.createOrReplaceTempView("logs")
    
    //获取一条经纬度信息
    val loc: DataFrame = spark.sql("select uuid, long, lat from logs where uuid = 862632030570300")
    import spark.implicits._
    val tupLoc: Dataset[(Double, Double)] = loc.map(x => {
      val long: Double = x.getString(1).toDouble
      val lat: Double = x.getString(2).toDouble
      (long, lat)
    })
    tupLoc


    //7)	商圈标签
    //获取geoHash
    

  }

}
