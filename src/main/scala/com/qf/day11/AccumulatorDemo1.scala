package com.qf.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object AccumulatorDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // foreach没有返回值，整个计算过程都是在Executor端执行的，此时sum值无法进行聚合
    val nums: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
//    var sum = 0
//    nums.foreach(num => {
//      sum += num
//    })
//    println(sum)

    // 简单实现累加功能
    // @deprecated("use AccumulatorV2", "2.0.0") 标记为过期
    // 以后要过期，不建议用
    val sum: Accumulator[Int] = sc.accumulator(0)
    nums.foreach(num => {
      sum += num
    })
    println(sum.value)

    sc.stop()
  }
}
