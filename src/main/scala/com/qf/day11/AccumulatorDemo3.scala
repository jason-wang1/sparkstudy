package com.qf.day11

import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo3").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(Array(1,2,3,4,5,6), 2)

    // 实例化自定义Accumulator
    val acc = new MyAccumulator()

    // 将Accumulator进行注册
    sc.register(acc, "acc")

    // 开始累加
    numbers.foreach(x => acc.add(x))
    println(acc.value)

    sc.stop()
  }
}
