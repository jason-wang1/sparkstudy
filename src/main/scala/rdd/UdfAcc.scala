package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输入：RDD[Int]
  * 输出：全量求和
  *
  * 使用自定义累加器对方式实现
  */
object UdfAcc {
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