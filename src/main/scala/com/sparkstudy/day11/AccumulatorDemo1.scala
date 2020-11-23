package com.sparkstudy.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: Executor端的计算无法更改Driver端的变量的值
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */
object AccumulatorDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("accumulatorDemo").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val coll: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

    var sum =0
    var sum2 = 0
    var sum3 = 0

    coll.foreach(x => sum += x)
    println(sum)

    coll.map(x => sum2+=x)
    println(sum2)

    for (i <- coll) sum3+=i
    println(sum3)

    val num1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6), 2)
    val num2 = sc.parallelize(Array(1.2, 5.3, 7.8, 5.6))

    def longAccumulator(name: String): LongAccumulator = {
      //创建用于累加的实例
      val acc = new LongAccumulator
      //注册
      sc.register(acc, name)
      acc
    }

    def doubleAccumulator(name: String): DoubleAccumulator = {
      val acc = new DoubleAccumulator
      sc.register(acc, name)
      acc
    }

    val acc1: LongAccumulator = longAccumulator("longsum")
    num1.foreach(x => acc1.add(x))
    println(acc1.value)

    val acc2: DoubleAccumulator = doubleAccumulator("doublesum")
    num2.foreach(x => acc2.add(x))
    println(acc2.value)

    sc.stop()

  }

}
