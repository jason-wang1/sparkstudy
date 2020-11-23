package com.qf.day12

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：根据用户访问的ip地址来统计所属区域并统计访问量
  * 思路：
  * 1、获取ip基本信息
  * 2、将ip进行广播
  * 3、获取用户点击流日志
  * 4、通过用户ip和基础ip端来判断用户所属区域
  * 5、通过得到的用户区域来统计访问量
  * 6、将结果持久化
  */
object IPSearch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ipsearch").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取ip基础数据
    val ipInfo = sc.textFile("D://data/ipsearch/ip.txt")

    // 切分ip数据
    val splitedIP = ipInfo.map(line => {
      val fields = line.split("\\|")
      val startip = fields(2)
      val endip = fields(3)
      val province = fields(6)
      (startip, endip, province)
    })

    // 将ip数据进行广播
    val broadcastIPInfo: Broadcast[Array[(String, String, String)]] = sc.broadcast(splitedIP.collect)

    // 获取用户数据
    val userInfo = sc.textFile("D://data/ipsearch/http.log")
    // 切分用户数据并查找该用户所属区域（省份）
    val provinceAnd1: RDD[(String, Int)] = userInfo.map(line => {
      val fields = line.split("\\|")
      val userIP = fields(1)
      val userIP2Long = ip2Long(userIP)
      val ipInfoArr: Array[(String, String, String)] = broadcastIPInfo.value
      val index = binarySearch(ipInfoArr, userIP2Long) // 通过二分查找，找到用户ip属于哪个区域的index
      val province = ipInfoArr(index)._3 // 获取到ip所属省份

      (province, 1)
    })

    // 统计区域访问量
    val sumed: RDD[(String, Int)] = provinceAnd1.reduceByKey(_+_)

    println(sumed.collect.toBuffer)

    sc.stop()
  }

  /**
    * 将ip转换为long类型
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val splited: Array[String] = ip.split("\\.")
    var ipNumber = 0L
    for (i <- 0 until splited.length) {
      ipNumber = splited(i).toLong | ipNumber << 8L
    }
    ipNumber
  }

  /**
    * 通过二分查找来查询ip对应的索引
    * @param arr
    * @param ip
    * @return
    */
  def binarySearch(arr: Array[(String, String, String)], ip: Long): Int = {
    // 设置起始值和结束值
    var start = 0
    var end = arr.length - 1

    while(start <= end) {
      // 求中间值
      val middle = (start + end) / 2
      // arr(middle) 获取数据中的中间那条数据的元组
      // 元组中的数据有：起始ip，结束ip，省份
      // 因为需要判断在ip的范围之内，所以需要取出元组中的值
      // 如果这个条件满足，就说明找到了这个ip
      if ((ip >= arr(middle)._1.toLong) && (ip <= arr(middle)._2.toLong)) {
        return middle
      }
      if (ip < arr(middle)._1.toLong) {
        end = middle - 1
      } else {
        start = middle + 1
      }
    }
    -1
  }


}
