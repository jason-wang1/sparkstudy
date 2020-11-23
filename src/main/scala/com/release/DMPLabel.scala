package com.release

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: 数据标签化
  * 过滤数据，将每天数据打上标签
  * Date: 2019年07月30日
  *
  * @author WangBo
  * @version 1.0
  */
object DMPLabel {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DMPLabel").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val logs: DataFrame = spark.read.parquet(Constant.PARQUET_PATH)
    logs.createOrReplaceTempView("logs")

    spark.sql("select uuid, long, lat from logs where long <> 0 and lat <> 0").show()

    //1)	广告位类型（标签格式： LC03->1 或者 LC16->1）xx 为数字，小于 10 补 0，把广告位类型名称，LN 插屏->1
    val adType: DataFrame = spark.sql("select uuid, " +
      "concat('LC', case when adspacetype < 10 then concat('0', adspacetype) else concat('', adspacetype) end, '->1 ', 'LN', adspacetypename, '->1') as flag_ad_type " +
      "from logs")
    adType.show()


    //2)	App 名称（标签格式： APPxxxx->1）xxxx 为 App 名称，使用缓存文件 appname_dict 进行名称转换；APP 爱奇艺->1
    val appName: DataFrame = spark.sql("select uuid, concat('APP', appname, '->1') from logs")
    appName.show()

    //3)	渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
    val source: DataFrame = spark.sql("select uuid, concat('CN', adplatformproviderid, '->1') from logs")
    source.show()


    //4)	设备：a)	(操作系统 -> 1)   b)	(联网方 -> 1)   c)	(运营商 -> 1)


    //5)	关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
    //超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签

    //6)	地域标签（省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）xxx 为省或市名称

    //7)	商圈标签


    //8)	上下文标签： 读取日志文件，将数据打上上述 6 类标签，并根据用户 ID 进行当前文件的合并，数据保存格式为：userId	K 青云志:3 D00030002:1 ……


    sc.stop()
    spark.stop()
  }

}
