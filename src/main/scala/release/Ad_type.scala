package com.release

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Description：xxxx
  * Copyright (c) ， 2019， John Will
  * This program is protected by copyright laws.
  * Date： 2019年07月30日
  *
  * @author LongZheng
  * @version : 1.0
  */
object Ad_type {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Ad_type")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val odsframe: DataFrame = spark.read.parquet("src/data/project_dmp_ods/dmp_ods")

    odsframe.createOrReplaceTempView("tb_ods")


    val resframe: DataFrame = spark.sql("select adspacetype,adspacetypename from tb_ods")

    val sql0 = "select * from tb_ods"

    resframe.createOrReplaceTempView("tb_tags")

    //1,广告位标签：最终的数据格式： 新增一列： 视频暂停悬浮09->1

    val resframe1: DataFrame = spark.sql("select *,concat('LC',adspacetypename,'LN',case  when adspacetype<10 then concat('0',adspacetype) else concat_ws('_',adspacetype) end,'->1') as  flag " +
      " from tb_tags")

    resframe1.createOrReplaceTempView("tb_tags1")

    val res2: DataFrame = spark.sql("select str_to_map(flag,'->') as str2map from tb_tags1")

    //2,APPname 标签  APPappname->1
    val sql2 = "select concat('APP',appname,'->','1') from tb_ods"


    //3,关键字标签
    /*
    5)	关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签

     */

    val frame: DataFrame = spark.read.text("src/data/dmp-project/stopwords.txt")
    import spark.implicits._
    val stopfream: DataFrame = frame.toDF("stop_words")
    stopfream.createOrReplaceTempView("tb_stopwords")

    val sql3 = "select value from tb_stopwords"

    val sql4 = "select keywords from tb_ods"

    //判断字段长度 len
    //注意：以'|'切分时会遇到问题：直接切且不出来，转移\\|也不行。需要使用[|]
    //第一步，将符合长度条件的words过滤出来
    val sql5 = "select uuid,case when length(trim(words))>=3 and length(trim(words))<=8 then trim(words) end as newwords from tb_ods " +
      "lateral view explode(split(keywords,'[|]'))t as words"

    val filter1: DataFrame = spark.sql(sql5)
    filter1.createOrReplaceTempView("tb_filter1")
    //第二步，将不在stopwords中的words再过滤一下，
    val sql7 = "select uuid,collect_list(concat(newwords,'->1')) from tb_filter1 a where not exists " +
      "(select 1 from tb_stopwords b where a.newwords=b.stop_words)  " +
      "group by uuid"

    val sql8 = "select uuid,collect_list(newwords)as newwords2 from tb_filter1 a where not exists " +
      "(select 1 from tb_stopwords b where a.newwords=b.stop_words)  " +
      "group by uuid"


    //regexp_replace(string INITIAL_STRING, string PATTERN, string REPLACEMENT)
    //测试：替换其中的中划线
    val sql6 = "select regexp_replace(keywords,'[|]',',')as newkeywords from tb_ods "


//    val res: DataFrame = spark.sql(sql8)
//    val res1: Dataset[Tag_keywords] = res.map(f => {
//      val fields: Array[String] = f.getString(2).split(",")
//      val tuples: Array[(String, Int)] = fields.map((_, 1))
//      val grouped: Map[String, Array[(String, Int)]] = tuples.groupBy(_._1)
//      val sumed = grouped.values.map(x=>{
//        x.reduce(_._2+_._2)
//      })
//      Tag_keywords(f.getString(1), sumed)
//    })
//    //res1
//    res1.show()
   // changed

    //changed.show(10,false)



    //res.show(10,false)



    //res.show(100, false)


    // resframe.show(10)
  }

}

case class Tag_keywords(uuid:String,keywords:ArrayBuffer[String])