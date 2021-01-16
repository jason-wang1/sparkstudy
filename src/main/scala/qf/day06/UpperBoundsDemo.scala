package com.qf.day06

/**
  * 上界
  */
class UpperBoundsDemo[T <: Comparable[T]] {  //T是后面的类的子类：T继承了Comparable
  def chooser(first: T, second: T): T = {  //两个T类型的对象之间的比较，返回我们想要的那个对象T
    if (first.compareTo(second) > 0) first else second  //一个粗略的比较规则，first是Girl中的this，second是Girl中的o
  }
}
object Chooser {
  def main(args: Array[String]): Unit = {
    val choose = new UpperBoundsDemo[Girl]  //自定义泛型Girl
    val g1 = new Girl("mimi", 80, 31)
    val g2 = new Girl("bingbing", 80, 34)

    val girl = choose.chooser(g1, g2)

    print(girl.name)
  }
}

class Girl(val name: String, val faceValue: Int, val age: Int) extends Comparable[Girl]{
  override def compareTo(that: Girl): Int = {  //对于Girl类的具体的比较规则：把我们想要的放在后面
    if (this.faceValue == that.faceValue) {
      that.age - this.age  //this年龄小为正，否则为负
    } else {
      this.faceValue - that.faceValue  //this颜值高为正，否则为负
    }
  }
}