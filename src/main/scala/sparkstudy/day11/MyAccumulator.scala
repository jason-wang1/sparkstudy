package com.sparkstudy.day11

import org.apache.spark.util.AccumulatorV2

/**
  * Descreption: 自定义累加器
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */

/**
  * 自定义Accumulator，要实现多个方法
  */
class MyAccumulator extends AccumulatorV2[Int, Int] {
  private var sum = 0

  override def isZero: Boolean = sum==0

  override def copy(): AccumulatorV2[Int, Int] = {
    val myacc = new MyAccumulator
    myacc.sum = this.sum
    myacc
  }

  override def reset(): Unit = 0

  override def add(v: Int): Unit = {
    sum+=v
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    sum += other.value
  }

  override def value: Int = sum
}
