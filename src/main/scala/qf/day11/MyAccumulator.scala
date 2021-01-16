package com.qf.day11

import org.apache.spark.util.AccumulatorV2

/**
  * 自定义Accumulator，需要实现多个方法
  */
class MyAccumulator extends AccumulatorV2[Int, Int]{
  // 创建一个输出值的变量
  private var sum: Int = _

  // 初始方法，检测是否为空
  override def isZero: Boolean = sum == 0

  // 拷贝一个新的累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    // 需要创建当前自定义累加器对象
    val myacc = new MyAccumulator
    // 需要将当前数据拷贝到新的累加器里面
    // 就是将原有累加器数据复制到新的累计器里面
    // 主要是为了数据的更新迭代
    myacc.sum = this.sum
    myacc
  }
  // 重置一个累加器，将累计器中的数据清零
  override def reset(): Unit = sum = 0
  // 局部累加方法，将每个分区进行分区内累加
  override def add(v: Int): Unit = {
    // v: 分区中的一个元素
    sum += v
  }
  // 全局聚合方法，将每个分区的结果进行累加
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    // other：分区的累加值
    sum += other.value
  }
  // 输出值--最终累加值
  override def value: Int = sum
}
