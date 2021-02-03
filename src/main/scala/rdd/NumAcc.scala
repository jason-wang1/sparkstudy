package rdd

import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输入：Long类型RDD / Double类型RDD
  * 输出：对它们求和
  *
  * 两种计算方式：1. 使用累加器；2. 使用reduce算子
  */
object NumAcc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /** 问题1：对整数类型RDD求和 */
    val num1 = sc.parallelize(Array(1,2,3,4,5,6), 2)

    //创建并注册一个longAccumulator，从0开始调用add累加
    def longAccumulator(name: String): LongAccumulator = {
      //创建累加的实例
      val acc = new LongAccumulator
      //注册
      sc.register(acc, name)

      acc
    }

    // 方式一：使用累加器
    val acc1: LongAccumulator = longAccumulator("longsum")
    num1.foreach(x => acc1.add(x))
    println(acc1.value)

    // 方式二：使用reduce算子
    val acc2: Int = num1.reduce(_+_)
    println(acc2)

    /** 问题2：对Double类型RDD求和 */
    val num2 = sc.parallelize(Array(1.1,2.2,3.3,4.4,5.5,6.6))

    //创建并注册一个doubleAccumulator
    def doubleAccumulator(name: String): DoubleAccumulator = {
      val acc = new DoubleAccumulator
      sc.register(acc, name)

      acc
    }

    // 方式一：使用累加器
    val acc3: DoubleAccumulator = doubleAccumulator("doubleAccumulator")
    num2.foreach(x => acc3.add(x))
    println(acc3.value)

    // 方式二：使用reduce算子
    val acc4 = num2.reduce(_+_)
    println(acc4)

  }

}
