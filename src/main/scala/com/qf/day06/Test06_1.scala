package com.qf.day06

import scala.io.Source

object Test06_1 {
  def main(args: Array[String]): Unit = {
    // 用fold方法将数组中的每个列表相同下标的元素进行聚合
    val arr = Array(List(2,3,4), List(1,2,3), List(4,5,6))
//    println(arr.fold(List(0, 0, 0))((x, y) => (x zip y).map(x => x._1+x._2))) // List((0,2),(0,3),(0,4))

    /**
      * 1）统计文件的行数
      * 2）统计出现“Spark”的行数
      * 3）统计“Spark”单词的个数
      */
    val content = Source.fromFile("c://data/test.txt").getLines()
    val lines = content.toList
    println(lines.size)
    println(lines.filter(_.contains("Spark")).size)
    println(lines.count(_.contains("Spark")))
    println(lines.flatMap(_.split(" ")).filter(_.contains("Spark")).size)
    println(lines.flatMap(_.split(" ")).count(_.equals("Spark")))




  }
}
