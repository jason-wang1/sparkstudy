package com.sparkstudy.day20

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月27日
  *
  * @author WangBo
  * @version 1.0
  */
object Test01 {
  def main(args: Array[String]): Unit = {
    val arr = Array(5, 2, 3, 9, 7)
    val tup: Array[(Int, Int)] = arr.zipWithIndex
    val sorted: Array[(Int, Int)] = tup.sortBy(_._1)
    println(sorted.toList)

    val sorted2: Array[((Int, Int), Int)] = sorted.zipWithIndex
    println(sorted2.toList)

    val tup2: Array[(Int, Int)] = sorted2.map(x => (x._1._2, x._2))
    val sorted3: Array[(Int, Int)] = tup2.sortBy(_._1)
    val index: Array[Int] = sorted3.map(_._2)
    println(index.toList)

  }

  //根据输入的数组输出数组每个元素升序排序后的下标
//  def sortIndex(arr: Array[Int]): Array[Int] = {
//    val tup: Array[(Int, Int)] = arr.zipWithIndex
//    val sorted: Array[(Int, Int)] = tup.sorted
//  }

}
