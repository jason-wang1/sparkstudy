package com.sparkstudy.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月02日
  *
  * @author WangBo
  * @version 1.0
  */
object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rddtest").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("hdfs:node01:9000/files")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val adidClick: RDD[(String, Int)] = sc.textFile("filePath/click.log")
      .map { item => {
        val splited: Array[String] = item.split("&")
        (splited(4).split("=")(1), 1)
      }}



    val adidNum: DataFrame = adidClick.reduceByKey(_+_)
      .toDF("adid", "click_num")


    val adidImp: RDD[(String, Int)] = sc.textFile("filePath/imp.log")
      .map { item => {
        val splited: Array[String] = item.split("&")
        (splited(4).split("=")(1), 1)
      }}

    val impNum: DataFrame = adidImp.reduceByKey(_+_)
      .toDF("adid", "imp_num")

    val clickAndImp: DataFrame = adidNum.join(impNum, "adid")

    clickAndImp.write.save("HDFSPath")
    clickAndImp.write.format("jdbc")
      .option("url", "url")
      .option("dbtable", "dftable")
      .option("user", "root")
      .option("password", "000000")
      .save()


  }
  case class clickAndImp(adid: String, clickNum: Int, impNum: Int)

}
