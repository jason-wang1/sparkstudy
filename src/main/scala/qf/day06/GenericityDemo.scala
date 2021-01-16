package com.qf.day06

/**
  * 实现一个普通的泛型
  * 需求: 比较两个自定义对象实例里的字段，谁大谁排在前面
  */
class GenericityDemo {

}

class Teacher(val name: String, val faceValue: Int) extends Comparable[Teacher]{
  override def compareTo(that: Teacher): Int = {
    this.faceValue - that.faceValue
  }
}

object Teacher{
  def main(args: Array[String]): Unit = {
    val t1 = new Teacher("dazhao", 90)
    val t2 = new Teacher("xiaodong", 990)

    val arr = Array(t1, t2)
    val sorted: Array[Teacher] = arr.sorted.reverse

    println(sorted(0).name)


  }
}



