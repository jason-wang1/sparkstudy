package com.qf.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object AccumulatorDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val num1 = sc.parallelize(Array(1,2,3,4,5,6), 2)
    val num2 = sc.parallelize(Array(1.1,2.2,3.3,4.4,5.5,6.6))

    // 创建并注册一个long accumulator， 从0开始，调用add方法进行累加
    def longAccumulator(name: String): LongAccumulator = {
      // 创建用于累加的实例
      val acc = new LongAccumulator
      // 开始注册
      sc.register(acc, name)
      acc
    }
    val acc1 = longAccumulator("longsum")
    num1.foreach(x => acc1.add(x)) // add方法进行累加
    println(acc1.value)

    // 创建并注册一个double accumulator， 从0开始，用add方法进行累加
    def doubleAccumulator(name: String): DoubleAccumulator = {
      // 创建用于累加的实例
      val acc = new DoubleAccumulator
      // 开始注册
      sc.register(acc, name)
      acc
    }
    val acc2 = doubleAccumulator("doublesum")
    num2.foreach(x => acc2.add(x)) // add方法进行累加
    println(acc2.value)


    sc.stop()
  }
}
