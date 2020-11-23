package com.qf.day11

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 实现单词计数的自定义Accumulator
  */
class MyAccumuletor2 extends AccumulatorV2[String, mutable.HashMap[String, Int]]{
  private val hashAcc = new mutable.HashMap[String, Int]()

  // 检测是否为空
  override def isZero: Boolean = hashAcc.isEmpty
  // 拷贝新的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new MyAccumuletor2
    hashAcc.synchronized {
      newAcc.hashAcc ++= hashAcc
    }
    newAcc
  }
  // 重置累加器
  override def reset(): Unit = hashAcc.clear()
  // 局部累加方法
  override def add(v: String): Unit = {
    hashAcc.get(v) match {
      case None => hashAcc += ((v, 1))
      case Some(x) => hashAcc += ((v, x + 1))
    }
  }
  // 全局累加
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case o: AccumulatorV2[String, mutable.HashMap[String, Int]] => {
        for ((k, v) <- o.value) {
          hashAcc.get(k) match {
            case None => hashAcc += ((k, v))
            case Some(x) => hashAcc += ((k, x + v))
          }
        }
      }
    }
  }
  // 输出值
  override def value: mutable.HashMap[String, Int] = hashAcc
}
