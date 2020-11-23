package com.quickscala

import java.awt.datatransfer.{DataFlavor, SystemFlavorMap}
import java.util.TimeZone

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Descreption: XXXX<br/>
  * Date: 2020年04月19日
  *
  * @author WangBo
  * @version 1.0
  */
object charpter03 {
  /*
   * 3.1
   * 编写一段代码，将a设置为一个n个随机整数的数组，要求随机数介于0和n之间。
   */
  def question1(n: Int) = {
    val arr: Array[Int] = (for (i <- 1 to n) yield Random.nextInt(n)).toArray
    arr
  }

  /*
   * 3.2
   * 编写一个循环，将整数数组中相邻的元素置换。
   */
  def question2(arr: Array[Int]): Array[Int] = {
    val l =arr.length
    for (i <- 0 to (if (l % 2 == 0) l - 2 else l - 3) if i % 2 == 0) {
      val temp = arr(i)
      arr(i) = arr(i + 1)
      arr(i + 1) = temp
    }
    arr
  }


  /*
   * 3.3
   * 重复前一个练习，不过这次生成一个新的值交换过的数组。用for/yield。
   */
  def question3(arr: Array[Int]): Array[Int] = {
    val ints: immutable.IndexedSeq[Int] = for (i <- 0 until arr.length) yield {
      if (i % 2 == 0) {
        if (i+1 < arr.length) arr(i + 1) else arr(i)
      } else {
        arr(i - 1)
      }
    }
    ints.toArray
  }


  /*
   * 3.4
   * 给定一个整数数组，产出一个新的数组，包含原数组中的所有正值，
   * 以原有顺序排列，之后的元素是所有零或负值，以原有顺序排列。
   */
  def question4(arr: Array[Int]) = {
    arr.filter(_ > 0) ++ arr.filter(_ <= 0)
  }


  /*
   * 3.5
   * 如何计算Array[Double]的平均值？
   */
  def question5(arr: Array[Int]) = {
    arr.sum / arr.length.toDouble
  }


  /*
   * 3.6
   * 如何重新组织Array[Int]的元素将它们反序排列？对于ArrayBuffer[Int]你又会怎么做呢？
   */
  def question6(arr: Array[Int]) = {
    arr.reverse
  }


  /*
   * 3.7
   * 编写一段代码，产出数组中的所有值，去掉重复项。
   */
  def question7(arr: Array[Int]) = {
    arr.distinct
  }


  /*
   * 3.8
   * 重新编写3.4节结尾的示例。收集负值元素的下标，反序，去掉最后一个下标，
   * 然后对每一个下标调用a.remove(i)。比较这样做的效率和3.4节中另外两种方法的效率。
   */
  def question8(arr: ArrayBuffer[Int]) = {

  }


  /*
   * 3.9
   * 创建一个由java.util.TimeZone.getAvailableIDs返回的时区集合，判断条件是它们在美洲，去掉”America/“前缀并排序。
   */
  def question9 = {
    java.util.TimeZone.getAvailableIDs
      .map(elem => if (elem.startsWith("America/")) elem.split("/").tail.mkString("/") else elem)
      .sorted
  }


  /*
   * 3.10
   * 引入java.awt.datatransfer._并构建一个类型为SystemFlavorMap类型的对象，
   * 然后以DataFlavor.imageFlavor为参数调用getNativesForFlavor方法，以Scala缓冲保存返回值。
   */
  def question10 = {

  }


  def main(args: Array[String]): Unit = {
    question9
      .foreach(println(_))
  }
}