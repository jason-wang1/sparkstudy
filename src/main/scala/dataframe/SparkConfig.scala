package com.dataframe

/**
  * Descreption: XXXX<br/>
  * Date: 2020年07月08日
  *
  * @author WangBo
  * @version 1.0
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfig {
  val APP_NAME: String = "app_name"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _


  def getSparkConf: SparkConf = {
    if(sparkConf == null) {
      val appName: String = APP_NAME
      sparkConf = new SparkConf().setAppName(appName)
      sparkConf.setMaster("local[*]")
    }
    sparkConf
  }


  def getSparkSession: SparkSession = {
    if(sparkSession == null)
      sparkSession = SparkSession.builder().enableHiveSupport().config(getSparkConf).getOrCreate()
    sparkSession
  }
}

