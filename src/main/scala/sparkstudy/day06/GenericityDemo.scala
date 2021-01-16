package com.sparkstudy.day06

/**
  * Descreption: 实现一个普通的泛型
  * 需求：比较两个自定义对象实例里的属性，并排序
  * Date: 2019年07月01日
  *
  * @author WangBo
  * @version 1.0
  */
class GenericityDemo {

}

class Teacher(val name: String, val faceValue: Int) extends Comparable[Teacher]{
  override def compareTo(o: Teacher): Int = {
    this.faceValue - o.faceValue  //升序排序
  }
}


object Teacher {
  def main(args: Array[String]): Unit = {
    val t1 = new Teacher("dazhao", 90)
    val t2 = new Teacher("xiaocang", 95)

    val arr = Array(t1, t2)
    val sorted: Array[Teacher] = arr.sorted.reverse  //降序排序

    println(sorted(0).name)
  }
}
