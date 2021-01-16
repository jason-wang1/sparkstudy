package com.sparkstudy.day11

import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: 使用官方定义类DoubleAccumulator, LongAccumulator实现数值累加
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */
object AccumulatorDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val num1 = sc.parallelize(Array(1,2,3,4,5,6), 2)
    val num2 = sc.parallelize(Array(1.1,2.2,3.3,4.4,5.5,6.6))

    //创建并注册一个longAccumulator，从0开始调用add累加
    def longAccumulator(name: String): LongAccumulator = {
      //创建累加的实例
      val acc = new LongAccumulator
      //注册
      sc.register(acc, name)

      acc
    }

    //使用longAccumulator
    val acc1: LongAccumulator = longAccumulator("longsum")
    num1.foreach(x => acc1.add(x))
    println(acc1.value)

    //创建并注册一个doubleAccumulator
    def doubleAccumulator(name: String): DoubleAccumulator = {
      val acc = new DoubleAccumulator
      sc.register(acc, name)

      acc
    }

    //使用doubleAccumulator
    val acc2: DoubleAccumulator = doubleAccumulator("doubleAccumulator")
    num2.foreach(x => acc2.add(x))
    println(acc2.value)
  }

}
