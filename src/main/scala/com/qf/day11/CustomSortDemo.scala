package com.qf.day11

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现自定义排序
  */
// 第一种排序方式
object MyOrdering {
  implicit val girlOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl) = {
      if (x.facevalue != y.facevalue)
        x.facevalue - y.facevalue
      else
        y.age - x.age
    }
  }
}

object CustomSortDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val girlInfo = sc.parallelize(List(("mimi", 90, 32),("bingbing", 85, 31),("yuanyuan", 80, 30)))

    // 需求：根据facevalue进行排序
//    val sorted = girlInfo.sortBy(_._2, false)
//    println(sorted.collect.toBuffer)

    // 需求：先按照facevalue进行排序，如果出现相等的情况，则按照age排序
    // 第一种自定义排序
//    import MyOrdering.girlOrdering
//    val sorted = girlInfo.sortBy(t => Girl(t._2, t._3))
//    println(sorted.collect.toBuffer)

    // 第二种排序方式
    val sorted = girlInfo.sortBy(t => Girl(t._2, t._3))
    println(sorted.collect.toBuffer)

    sc.stop()
  }
}

// 第一种排序方式
//case class Girl(facevalue: Int, age: Int)

// 第二种排序方式
case class Girl(facevalue: Int, age: Int) extends Ordered[Girl] {
  override def compare(that: Girl): Int = {
    if (this.facevalue == that.facevalue) that.age - this.age
    else this.facevalue - that.facevalue
  }
}

