package com.release

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: 统计各省市数据量分布情况
  *
  * @author WangBo
  * @version 1.0
  */
object DMPDataDistributed {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DMPDataDistributed").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //将统计的结果输出成 json 格式，并输出到磁盘目录
    val logs: DataFrame = spark.read.parquet(Constant.PARQUET_PATH)
    logs.createOrReplaceTempView("logs")

    val area: DataFrame = spark.sql("select count(*) as ct, provincename, cityname " +
      "from logs group by provincename, cityname order by ct desc").repartition(1)
    area.show()
//    area.write.json(Constant.COUNT_BY_AREA)

    //将结果写到到 mysql 数据库
    val props = new Properties()
    props.put("url", "jdbc:mysql://NODE01:3306/test?useUnicode=true&characterEncoding=utf8")  //test为数据库名称
    props.put("user", "root")
    props.put("password", "root")
    props.put("driver", "com.mysql.jdbc.Driver")
    area.write.jdbc("jdbc:mysql://NODE01:3306/test?useUnicode=true&characterEncoding=utf8", "DMP_area", props)
//    area.write.format("jdbc")
//      .option("url", "jdbc:mysql://NODE01:3306/test?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable", "DMP_area")
//      .option("user", "root")
//      .option("password", "root")
//      .save()

    //用 spark 算子的方式实现上述的统计，存储到磁盘
    val rdd: RDD[Row] = logs.rdd
    val areaRDD: RDD[(String, Int)] = rdd.map(line => {
      val provincename: String = line.getString(24)
      val cityname: String = line.getString(25)
      (provincename + ":" + cityname, 1)
    })
    val countRDD: RDD[(String, Int)] = areaRDD.reduceByKey(_ + _).sortBy(_._2, false)
    println(countRDD.collect().toList)

    //地域主题下的竞价、点击、成本
    spark.sql("select provincename, cityname, " +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) as flag_org, " +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) as flag_eff, " +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 3 then 1 else 0 end) as flag_ad, " +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) as flag_isbid, " +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) as flag_iswin, " +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) as flag_show, " +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) as flag_click " +
      "from logs " +
      "group by provincename, cityname").show()


    sc.stop()
    spark.stop()
  }
}
