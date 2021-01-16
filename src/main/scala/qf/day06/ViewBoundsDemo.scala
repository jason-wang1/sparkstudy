package com.qf.day06

/**
  * 视界
  */
class ViewBoundsDemo[T <% Ordered[T]] {  //Ordered：比较的类，实现了Java的Comparable接口，里面的方法有 > < >= <=
  def chooser(first: T, second: T): T = {
    if (first > second) first else second  //给了一个大致的比较，具体的比较规则用隐式转换函数
  }
}
object ViewBoundsDemo {
  def main(args: Array[String]): Unit = {
    import MyPredef.girlSelect

    val choose = new ViewBoundsDemo[MyGirl]
    val g1 = new MyGirl("mimi", 90, 31)
    val g2 = new MyGirl("yuanyuan", 90, 30)

    val girl = choose.chooser(g1, g2)

    println(girl.name)
  }
}

//有了隐式转换函数来指定比较规则，这里就不用再定义了
class MyGirl(val name: String, val faceValue: Int, val age: Int) {}
