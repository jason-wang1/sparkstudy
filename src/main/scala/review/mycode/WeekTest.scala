package com.review.mycode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年09月05日
  *
  * @author WangBo
  * @version 1.0
  */
object WeekTest {

  case class User(user: String, areacode: String, gender: String)
  case class Action(user: String, action: String, action_target: String, ct: String)

//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("WeekTest").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//    spark.sql("select areacode, gender, ulevel " +
//      "from user_level_statis")
//
//    val ods02User: DataFrame = spark.sql("select * from ods_test.ods_02_user")
//    val ods_user_action_log: DataFrame = spark.sql("select * from ods_user_action_log " +
//      "where bdp_day > 20190902 and bdp_day < 20190908")
//
//    val odsUserRDD: RDD[Row] = ods02User.rdd
//    val odsUserActionLogRDD: RDD[Row] = ods_user_action_log.rdd
//
//    odsUserActionLogRDD.map(row => {
//      val user = row.getAs("user")
//      val action = row.getAs("action")
//      var score: Int = _
//
//      action match {
//        case "01" => score = 1
//        case "02" => score = 2
//        case "03" => score = 2
//        case "04" => score = 3
//        case "05" => score = 5
//      }
//      (user, score)
//    })
//      .reduceByKey(_+_)
//      .map{case (user, uscore) => {
//        var ulevel: String = _
//        if (uscore < 1000) {
//          ulevel = "普通"
//        }
//        else if (uscore < 3000) {
//          ulevel = "银牌"
//        }
//        else if (uscore < 10000) {
//          ulevel = "金牌"
//        }
//        else {
//          ulevel = "钻石"
//        }
//        (user, uscore, ulevel)
//      }}
//
//  }

}
