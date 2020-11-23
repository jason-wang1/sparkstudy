package com.sparkstudy.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: 使用自定义累加器
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */
object AccumulatorDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("accumulatorDemo").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val numbers: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6), 2)

    println("查看原始数据分区：")
    //输出集合中元素的分区
    val func = (index: Int, it: Iterator[Int]) => {
      it.map(x => "index: "+index+", value:"+x)
    }
    val numsPart: RDD[String] = numbers.mapPartitionsWithIndex(func)
    numsPart.collect().toList.foreach(println)

    //实例化自定义累加器
    val acc = new MyAccumulator

    //注册累加器
    sc.register(acc, "intAcc")

    //开始累加
    numbers.foreach(x => acc.add(x))
    println("累加的值为："+acc.value)

    sc.stop()
  }
}
