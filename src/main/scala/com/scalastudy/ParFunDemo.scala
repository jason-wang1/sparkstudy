package com.scalastudy

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月19日
  *
  * @author WangBo
  * @version 1.0
  */
object ParFunDemo {
  def main(args: Array[String]): Unit = {
    val list: List[Any] = List(1, 2, 3, 4, "hello")

    //PartialFunction[Any, Int] 表示接收的数据类型是Any,返回的数据类型是Int
    //isDefinedAt(x: Any)如果返回true，则调用apply方法；如果是false，则过滤
    val parFun: PartialFunction[Any, Int] = new PartialFunction[Any, Int] {
      override def isDefinedAt(x: Any): Boolean = x.isInstanceOf[Int]

      override def apply(v1: Any): Int = v1.asInstanceOf[Int] + 1
    }

    //map不能使用偏函数
    val list2: List[Int] = list.collect(parFun)
    println(list2)
  }
}
