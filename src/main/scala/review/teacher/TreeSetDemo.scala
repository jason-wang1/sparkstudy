package com.review.teacher

import scala.collection.immutable

object TreeSetDemo {
  def main(args: Array[String]): Unit = {
    var set = immutable.TreeSet[(Double, String)]()
    set = immutable.TreeSet[(Double, String)]()
    val elem1: (Double, String) = (10.0, "ddd")
    val set1 = set + elem1

    val elem2: (Double, String) = (9.0, "aaa")
    val set2 = set1 + elem2

    val elem3: (Double, String) = (10.0, "bbb")
    val set3 = set2 + elem3

    println(set3)


  }
}
