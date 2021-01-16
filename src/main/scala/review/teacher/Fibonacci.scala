package com.review.teacher

object Fibonacci {
  def main(args: Array[String]): Unit = {
    val N = 60

    val result2 = func2(N)
    println(result2)

    val arrTmp = new Array[Long](N+1)
    val result3 = func3(N, arrTmp)
    println(result3)

    val result1 = func1(N)
    println(result1)

  }

  // 普通的递归
  def func1(n: Int): Long ={
    assert(n>=1)
    if (n==1) 1
    else if (n==2) 2
    else func1(n-1) + func1(n-2)
  }

  // 循环
  def func2(n: Int): Long ={
    assert(n>=1)

    var a = 1L
    var b = 2L
    if (n==1) return a
    else if (n==2) return b

    var result = 0L
    for (i <- 3 to n) {
      result = a + b
      a = b
      b = result
    }
    result
  }

  // 备忘录法(递归，用数组保存中间结果)
  def func3(n: Int, arr: Array[Long]): Long ={
    assert(n>=1)
    if (n==1) 1
    else if (n==2) 2
    else if (arr(n)!=0) arr(n)
    else {
      arr(n) = func3(n-1, arr) + func3(n-2, arr)
      arr(n)
    }
  }

}
