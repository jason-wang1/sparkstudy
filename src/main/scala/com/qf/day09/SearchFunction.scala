package com.qf.day09

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class SearchFunction (val query: String) {
  // 第一个方法判断输入的字符串是否存在query
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchFuncRef(rdd: RDD[String]): RDD[String] = {
    // 在这儿，调用isMatch方法，相当于this.isMatch, 需要将this对象传给Executor
    rdd.filter(this.isMatch)
  }

  def getMatchFieldRef(rdd: RDD[String]): RDD[String] = {
    // 在这儿，相当于将this传给Executor
    rdd.filter(_.contains(this.query))
  }

  def getMatchNoRef(rdd: RDD[String]): RDD[String] = {
    val _query = query
    rdd.filter(_.contains(_query))
  }
}
object SearchFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SearchFunction").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List("hello java", "hello scala"))

    val sf = new SearchFunction("java")

//    sf.getMatchFuncRef(rdd)
//    sf.getMatchFieldRef(rdd)
    sf.getMatchNoRef(rdd)

  }
}
