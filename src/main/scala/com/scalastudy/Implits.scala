package com.scalastudy

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月14日
  *
  * @author WangBo
  * @version 1.0
  */
object Implits {
  def main(args: Array[String]): Unit = {
    implicit def int2String(x: Int) = x.toString

    def foo(msg: String) = println(msg)
    foo(10)
  }
}
