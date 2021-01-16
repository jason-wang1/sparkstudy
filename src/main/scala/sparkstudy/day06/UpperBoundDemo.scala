package com.sparkstudy.day06

/**
  * Descreption: 上界
  * Date: 2019年07月01日
  *
  * @author WangBo
  * @version 1.0
  */
class UpperBoundDemo [T <: Comparable[T]] {
  def chooser(first: T, second: T): T = {
    if (first.compareTo(second) > 0) first else second
  }
}

object chooser {
  def main(args: Array[String]): Unit = {
    val choose = new UpperBoundDemo[Girl]
    val g1 = new Girl("mimi", 80, 31)
    val g2 = new Girl("cang", 90, 35)

    println(choose.chooser(g1, g2).name)
  }
}

class Girl(val name: String, val faceValue: Int, val age: Int) extends Comparable[Girl] {
  override def compareTo(o: Girl): Int = {
    if (this.faceValue == o.faceValue) {
      o.age - this.age
    } else {
      this.faceValue - o.faceValue
    }
  }
}