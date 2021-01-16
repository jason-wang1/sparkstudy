package com.qf.day06

/**
  * 上下文界定
  */
class ContextBoundsDemo[T : Ordering] {
  def chooser(first: T, second: T): T = {
    val ord: Ordering[T] = implicitly[Ordering[T]]
    if (ord.gt(first, second)) first else second
  }
}
object ContextBoundsDemo {
  def main(args: Array[String]): Unit = {
//    import MyPredef.OrderingGirl
    import MyPredef.girlSelect

    val choose = new ContextBoundsDemo[MyGirl]
    val g1 = new MyGirl("ruhua", 50, 26)
    val g2 = new MyGirl("xiaoyueyue", 50, 22)

    val girl = choose.chooser(g1, g2)

    println(girl.name)

  }
}
