package com.qf.day12

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCRDDTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sql = "select id, location, counts, access_date from location_info where id >= ? and id <= ?"

    // jdbc连接驱动设置
    val jdbcurl = "jdbc:mysql://node03:3306/bigdata?useUnicode=true&charsetEncoding=utf8"
    val user = "root"
    val password = "root"

    // 获取数据库的连接
    val conn = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection(jdbcurl, user, password)
    }

    // 调用JdbcRDD获取mysql的数据
    val jdbcRDD = new JdbcRDD(
      sc,
      conn,
      sql,
      0,
      1000,
      1,
      res => {
        val id = res.getInt("id")
        val location = res.getString("location")
        val counts = res.getInt("counts")
        val access_date = res.getDate("access_date")
        (id, location, counts, access_date)
      }
    )

    jdbcRDD.foreach(println)

    sc.stop()
  }
}
