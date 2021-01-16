package com.release

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Descreption: 广告投放项目用户主题ods层
  * Date: 2019年07月25日
  *
  * @author WangBo
  * @version 1.0
  */
object ReleaseCustomer {

  //解析exts的json字符串
  def parJson(exts: String)= {
    val extsJason: JSONObject = JSON.parseObject(exts)
    val idcard: String = extsJason.get("idcard").toString
    val longitude: String = extsJason.get("longitude").toString
    val latitude: String = extsJason.get("latitude").toString
    val area_code: String = extsJason.get("area_code").toString
    val matter_id: String = extsJason.get("matter_id").toString
    val model_code: String = extsJason.get("model_code").toString
    val model_version: String = extsJason.get("model_version").toString
    val aid: String = extsJason.get("aid").toString
    (idcard, longitude, latitude, area_code, matter_id, model_code, model_version, aid)
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AdReleaseCustomer").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//    val sc = new SparkContext(conf)
    import spark.implicits._

    /**
      * 获取数据到ods层
      */
    val dfODS: DataFrame = spark.read.parquet("D://data/day13")
    dfODS.show(10)
    dfODS.printSchema()
    /**
      * 转换数据到dw层
      */
    //取出release_status为00(非目标客户)或01(目标客户)的数据
    val filted: Dataset[Row] = dfODS.filter(x => x.getString(3).substring(1).toInt<2)
    filted.show(10)

    val dsDW: Dataset[DwCustomer] = filted.map(x => {
      val release_session: String = x.getString(2)
      val release_status: String = x.getString(3)
      val device_num: String = x.getString(1)
      val device_type: String = x.getString(4)
      val sources: String = x.getString(5)
      val channels: String = x.getString(6)
      val exts: String = x.getString(7)
      val parsed: (String, String, String, String, String, String, String, String) = parJson(exts)
      val idcard: String = parsed._1
      val age: Int = 2019 - idcard.substring(6, 10).toInt
      var gender: String = null
      if (parsed._1.length == 17) {
        val genderInt: Int = idcard.substring(16, 17).toInt
        if (genderInt % 2 == 0) {
          gender = "女"
        } else {
          gender = "男"
        }
      } else {
        val genderInt: Int = idcard.substring(14, 15).toInt
        if (genderInt % 2 == 0) {
          gender = "女"
        } else {
          gender = "男"
        }
      }
      val ct: Long = x.getLong(8)
      DwCustomer(release_session, release_status, device_num, device_type, sources, channels,
        age, gender, idcard, parsed._4, parsed._2, parsed._3, parsed._5, parsed._6, parsed._7, parsed._8, ct)
    })

    dsDW.show(10)

    /**
      * 转换到dm层
      */
    //不同渠道的目标用户与非目标用户计数
    val grouped: RelationalGroupedDataset = dsDW.groupBy($"sources", $"release_status")
    val dmCustom: DataFrame = grouped.count()
    dmCustom.show()

    spark.stop()
//    sc.stop()
  }
}

case class DwCustomer(release_session: String, release_status: String, device_num: String, device_type: String,
                      sources: String, channels: String, age: Int, gender: String, idcard: String, area_code: String,
                      longitude: String, latitude: String, matter_id: String, model_code: String, model_version: String,
                      aid: String, ct: Long)
