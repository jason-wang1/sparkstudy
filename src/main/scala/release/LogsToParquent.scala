package com.release

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * 一、日志转Parquet 文件
  * 1)	要求一： 将数据转换成 parquet 文件格式
  * 2)	要求二： 序列化方式采用 KryoSerializer 方式
  * 3)	要求三： parquet 文件采用 Snappy 压缩方式
  *
  * @author WangBo
  * @version 1.0
  */
object LogsToParquent {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LogsToParquent").setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val logs: RDD[String] = sc.textFile("D://data/2016-10-01_06_p1_invalid.1475274123982.log.FINISH")
    logs.collect().take(5).toList.foreach(println)

    /**
      * 日志转Parquet文件。
      */

    val splited: RDD[Array[String]] = logs.map(_.split(",")).filter(_.length==85)

    //把Array转换成Row
    val logsRowRDD: RDD[Row] = splited.map(arr => {
      Row(
        arr(0),
        if(arr(1).equals("")) 0 else arr(1).toInt,
        if(arr(2).equals("")) 0 else arr(2).toInt,
        if(arr(3).equals("")) 0 else arr(3).toInt,
        if(arr(4).equals("")) 0 else arr(4).toInt,
        arr(5), arr(6),
        if(arr(7).equals("")) 0 else arr(7).toInt,
        if(arr(8).equals("")) 0 else arr(8).toInt,
        if(arr(9).equals("")) 0.0 else arr(9).toDouble,
        arr(10).toDouble,
        arr(11), arr(12), arr(13), arr(14), arr(15), arr(16),
        if(arr(17).equals("")) 0 else arr(17).toInt,
        arr(18), arr(19),
        if(arr(20).equals("")) 0 else arr(20).toInt,
        if(arr(21).equals("")) 0 else arr(21).toInt,
        arr(22), arr(23), arr(24), arr(25),
        if(arr(26).equals("")) 0 else arr(26).toInt,
        arr(27),
        if(arr(28).equals("")) 0 else arr(28).toInt,
        arr(29),
        if(arr(30).equals("")) 0 else arr(30).toInt,
        if(arr(31).equals("")) 0 else arr(31).toInt,
        if(arr(32).equals("")) 0 else arr(32).toInt,
        arr(33),
        if(arr(34).equals("")) 0 else arr(34).toInt,
        if(arr(35).equals("")) 0 else arr(35).toInt,
        if(arr(36).equals("")) 0 else arr(36).toInt,
        arr(37),
        if(arr(38).equals("")) 0 else arr(38).toInt,
        if(arr(39).equals("")) 0 else arr(39).toInt,
        if(arr(40).equals("")) 0.0 else arr(40).toDouble,
        if(arr(41).equals("")) 0.0 else arr(41).toDouble,
        if(arr(42).equals("")) 0 else arr(42).toInt,
        arr(43),
        if(arr(44).equals("")) 0.0 else arr(44).toDouble,
        if(arr(45).equals("")) 0.0 else arr(45).toDouble,
        arr(46), arr(47), arr(48), arr(49),
        arr(50), arr(51), arr(52), arr(53), arr(54), arr(55), arr(56),
        if(arr(57).equals("")) 0 else arr(57).toInt,
        if(arr(58).equals("")) 0.0 else arr(58).toDouble,
        if(arr(59).equals("")) 0 else arr(59).toInt,
        if(arr(60).equals("")) 0 else arr(60).toInt,
        arr(61), arr(62), arr(63), arr(64), arr(65), arr(66), arr(67), arr(68), arr(69),
        arr(70), arr(71), arr(72),
        if(arr(73).equals("")) 0 else arr(73).toInt,
        if(arr(74).equals("")) 0.0 else arr(74).toDouble,
        if(arr(75).equals("")) 0.0 else arr(75).toDouble,
        if(arr(76).equals("")) 0.0 else arr(76).toDouble,
        if(arr(77).equals("")) 0.0 else arr(77).toDouble,
        if(arr(78).equals("")) 0.0 else arr(78).toDouble,
        arr(79),
        arr(80), arr(81), arr(82), arr(83),
        if(arr(84).equals("")) 0 else arr(84).toInt
      )
    })

    // 通过StructType的方式生成Schema
    val schema = StructType(Array(
      StructField("sessionid", StringType, true), //0
      StructField("advertiserid", IntegerType, true),
      StructField("adorderid", IntegerType, true),
      StructField("adcreativeid", IntegerType, true),
      StructField("adplatformproviderid", IntegerType, true),
      StructField("sdkversion", StringType, true),
      StructField("adplatformkey", StringType, true),
      StructField("putinmodeltype", IntegerType, true),
      StructField("requestmode", IntegerType, true),
      StructField("adprice", DoubleType, true),
      StructField("adppprice", DoubleType, true), //10
      StructField("requestdate", StringType, true),
      StructField("ip", StringType, true),
      StructField("appid", StringType, true),
      StructField("appname", StringType, true),
      StructField("uuid", StringType, true),
      StructField("device", StringType, true),
      StructField("client", IntegerType, true),
      StructField("osversion", StringType, true),
      StructField("density", StringType, true),
      StructField("pw", IntegerType, true),  //20
      StructField("ph", IntegerType, true),
      StructField("long", StringType, true),
      StructField("lat", StringType, true),
      StructField("provincename", StringType, true),
      StructField("cityname", StringType, true),
      StructField("ispid", IntegerType, true),
      StructField("ispname", StringType, true),
      StructField("networkmannerid", IntegerType, true),
      StructField("networkmannername", StringType, true),
      StructField("iseffective", IntegerType, true),  //30
      StructField("isbilling", IntegerType, true),
      StructField("adspacetype", IntegerType, true),
      StructField("adspacetypename", StringType, true),
      StructField("devicetype", IntegerType, true),
      StructField("processnode", IntegerType, true),
      StructField("apptype", IntegerType, true),
      StructField("district", StringType, true),
      StructField("paymode", IntegerType, true),
      StructField("isbid", IntegerType, true),
      StructField("bidprice", DoubleType, true),  //40
      StructField("winprice", DoubleType, true),
      StructField("iswin", IntegerType, true),
      StructField("cur", StringType, true),
      StructField("rate", DoubleType, true),
      StructField("cnywinprice", DoubleType, true),
      StructField("imei", StringType, true),
      StructField("mac", StringType, true),
      StructField("idfa", StringType, true),
      StructField("openudid", StringType, true),
      StructField("androidid", StringType, true),  //50
      StructField("rtbprovince", StringType, true),
      StructField("rtbcity", StringType, true),
      StructField("rtbdistrict", StringType, true),
      StructField("rtbstreet", StringType, true),
      StructField("storeurl", StringType, true),
      StructField("realip", StringType, true),
      StructField("isqualityapp", IntegerType, true),
      StructField("bidfloor", DoubleType, true),
      StructField("aw", IntegerType, true),
      StructField("ah", IntegerType, true),  //60
      StructField("imeimd5", StringType, true),
      StructField("macmd5", StringType, true),
      StructField("idfamd5", StringType, true),
      StructField("openudidmd5", StringType, true),
      StructField("androididmd5", StringType, true),
      StructField("imeisha1", StringType, true),
      StructField("macsha1", StringType, true),
      StructField("idfasha1", StringType, true),
      StructField("openudidsha1", StringType, true),
      StructField("androididsha1", StringType, true),  //70
      StructField("uuidunknow", StringType, true),
      StructField("userid", StringType, true),
      StructField("iptype", IntegerType, true),
      StructField("initbidprice", DoubleType, true),
      StructField("adpayment", DoubleType, true),
      StructField("agentrate", DoubleType, true),
      StructField("lomarkrate", DoubleType, true),
      StructField("adxrate", DoubleType, true),
      StructField("title", StringType, true),
      StructField("keywords", StringType, true),  //80
      StructField("tagid", StringType, true),
      StructField("callbackdate", StringType, true),
      StructField("channelid", StringType, true),
      StructField("mediatype", IntegerType, true)

    ))

    val df: DataFrame = spark.createDataFrame(logsRowRDD, schema)
    df.show()



//    df.write.parquet(Constant.PARQUET_PATH)

    sc.stop()
    spark.stop()
  }
}
