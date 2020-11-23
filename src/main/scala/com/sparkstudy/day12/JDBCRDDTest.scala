package com.sparkstudy.day12

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月09日
  *
  * @author WangBo
  * @version 1.0
  */
object JDBCRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("JDBCRDDTest").setMaster("local[2]")
    val sc = new SparkContext(conf)


  }

}
