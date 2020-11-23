package com.sparkstudy.day12

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption:
  * 需求：根据点击日志中用户的ip地址来统计所属区域并统计访问量
  * 思路：
  * 1 获取ip基本信息“ip.txt”
  * 2 将ip广播
  * 3 获取用户点击流日志“http.log”
  * 4 通过用户ip和基础ip来判断用户所属区域
  * 5 通过得到的用户区域来统计访问量
  * 6 将结果持久化
  * Date: 2019年07月09日
  *
  * @author WangBo
  * @version 1.0
  */
object IPSearch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("ipsearch").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //获取ip基础数据
    val ipInfo: RDD[String] = sc.textFile("D://data/ipsearch/ip.txt")

    //切分ip数据  (ip起点，ip终点，所属省份)
    val splitedIp: RDD[(String, String, String)] = ipInfo.map(line => {
      val fields: Array[String] = line.split("\\|")
      val startip: String = fields(2)
      val endip: String = fields(3)
      val province: String = fields(6)

      (startip, endip, province)
    })

    //将ip数据广播出去
    val broadcastIpInfo: Broadcast[Array[(String, String, String)]] = sc.broadcast(splitedIp.collect())

    //获取用户点击日志
    val userInfo: RDD[String] = sc.textFile("D://data/ipsearch/http.log")
    //切分用户数据并查找该用户所属省份
    val provinceAnd1: RDD[(String, Int)] = userInfo.map(line => {
      val fields: Array[String] = line.split("\\|")
      val userIp: String = fields(1)
      val userIp2Long: Long = ip2Long(userIp)
      val ipInfoArr: Array[(String, String, String)] = broadcastIpInfo.value
      val index: Int = binarySearch(ipInfoArr, userIp2Long) //通过二分查找，找到用户ip属于哪个区域的index
      val province: String = broadcastIpInfo.value(index)._3

      (province, 1)
    })

    //统计各省份用户访问量
    val sumed: RDD[(String, Int)] = provinceAnd1.reduceByKey(_ + _)

    sumed.collect().toList.foreach(println)

    sc.stop()

  }

  //将ip地址转换成Long类型
  def ip2Long(userIp: String): Long = {
    val splited: Array[String] = userIp.split("\\.")
    var ipNum = 0L
    for (i <- 0 until splited.length) {
      ipNum = splited(i).toLong | ipNum << 8
    }
    ipNum
  }

  /**
    *
    * @param tuples Array[(ip起点，ip终点，所属省份)]
    * @param ip 用户ip地址
    * @return 用户ip属于哪个省份的index
    */
  def binarySearch(tuples: Array[(String, String, String)], ip: Long): Int = {
    //设置指针
    var start = 0
    var end = tuples.length - 1
     while (start <= end) {
       //求中间值
       var middle = (start + end) / 2

       if (ip >= tuples(middle)._1.toLong && ip <= tuples(middle)._2.toLong) {
         return middle
       }
       if (ip < tuples(middle)._1.toLong) {
         end = middle - 1
       }
       else {
         start = middle + 1
       }
     }
    -1
  }
}
