package com.qf.day07

object Test07_1 {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,1,2,2,4,5)

    println(getNum(arr, 3))
    println(swap(arr).toBuffer)

  }

  // 实现一个方法，使得返回数组中大于n的元素个数，等于n的元素个数，
  // 小于n的元素个数，并且一起返回。
  def getNum(arr: Array[Int], n: Int): (Int, Int, Int) = {
    val n1 = arr.count(_>n)
    val n2 = arr.count(_==n)
    val n3 = arr.count(_<n)
    (n1, n2, n3)
  }

  // 实现一个方法，使得传进入的数组的元素两两交换。
  def swap(arr: Array[Int]): Array[Int] = {
    for (i <- 0 until(arr.length-1, 2)) {
      val temp = arr(i)
      arr(i) = arr(i + 1)
      arr(i + 1) = temp
    }
    arr
  }

}
