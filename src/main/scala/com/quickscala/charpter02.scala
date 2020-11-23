package com.quickscala

/**
  * Descreption: XXXX<br/>
  * Date: 2020年04月19日
  *
  * @author WangBo
  * @version 1.0
  */
object charpter02 {
  /*
   * 2.1
   * 一个数字如果为正数，则它的signum为1;
   * 如果是负数,则signum为-1;
   * 如果为0,则signum为0.编写一个函数来计算这个值
   * */
  def signum(i: Int): Int = {
    if (i > 0) 1
    else if (i < 0) -1
    else 0
  }

  /*
   * 2.2
   * 一个空的块表达式{}的值是什么？类型是什么？
   * */
  def block(): Unit = {}


  /*
   * 2.3
   * 指出在Scala中何种情况下赋值语句x=y=1是合法的。
   * (提示：给x找个合适的类型定义)
   */
  def checkAssignLegal() = {

    var x = ()
    println(s"x's type is: ${x.getClass}")
    var y = 1
    x = y = 1
    println(x)
  }

  /*
   * 2.4
   * 针对下列Java循环编写一个Scala版本:
   * for(int i=10;i>=0;i–-)System.out.println(i);
   */
  def ScalaForeach() = {
    {1 to 10}.reverse.foreach(println(_))
  }

  /*
   * 2.5
   * 编写一个过程countdown(n:Int)，打印从n到0的数字
   */
  def countdown(n:Int) = {
    {0 to n}.reverse.foreach(println(_))
  }


  /*
   * 2.6
   * 编写一个for循环,计算字符串中所有字母的Unicode代码的乘积。
   * 举例来说，"Hello"中所有字符串的乘积为9415087488L
   */
  def calculateCharsUnicodeProduct(s: String) = {
    var res = 1L
    s.foreach( res *= _.toLong)
    res
  }


  /*
   * 2.7
   * 同样是解决前一个练习的问题，但这次不使用循环。
   * （提示：在Scaladoc中查看StringOps）
   */
  def computeCharsUnicodeProduct(s: String) = {
    s.foldLeft(1L)((res, c) => res * c.toLong)
  }


  /*
   * 2.8
   * 编写一个函数product(s:String)，
   * 计算前面练习中提到的乘积
   * 2.9
   * 把前一个练习中的函数改成递归函数
   */
    def product(s: String): Long = {
      if (s.length() == 1) {
        s(0) toLong
      } else {
        s(0).toLong * product(s.tail)
      }
    }



  /*
   * 2.10
   * 编写函数计算x^n,其中n是整数，使用如下的递归定义:
   */
  def question10(x: Int, n: Int): Double = n match {
    case 0 => 1
    case _ if n % 2 == 0 => question10(x, n/2) * question10(x, n/2)
    case _ if n % 2 == 1 => question10(x, n-1) * x
    case _ if n < 0 => 1.0 / question10(x, -n)
  }


  def main(args: Array[String]): Unit = {
//    // 2.1
//    println(signum(0))

//    // 2.2
//    println(block())
//    println(block().getClass)

//    // 2.3
//    checkAssignLegal

//    // 2.4
//    ScalaForeach

//    // 2.5
//    countdown(5)

//    // 2.6
//    println(calculateCharsUnicodeProduct("Hello"))

//    // 2.7
//    println(computeCharsUnicodeProduct("Hello"))

//    // 2.8
//    println(product("Hello"))

    // 2.9
    println(question10(3, 4))
  }
}