package com.scalastudy

/**
  * Descreption: XXXX<br/>
  * Date: 2020年04月15日
  *
  * @author WangBo
  * @version 1.0
  */
object ScalaPuzzlers {
  def main(args: Array[String]): Unit = {
    val ints: List[Int] = List(1, 2).map { i => println("Hi"); i + 1 }
    println(ints)
    val ints2: List[Int] = List(1, 2).map { println("Hi"); _ + 1 }
    println(ints2)
  }
}
