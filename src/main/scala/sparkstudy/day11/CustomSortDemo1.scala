package com.sparkstudy.day11

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Descreption: 实现自定义排序1
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */
//自定义排序方式1
object CustomSortDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("customSort").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val girlInfo: RDD[(String, Int, Int)] = sc.parallelize(List(("mimi", 90, 32), ("bingbing", 85, 31), ("yuanyuan", 90, 30)))

    import MyOrdering.girlOrdering
    val sorted: RDD[(String, Int, Int)] = girlInfo.sortBy(t => Girl(t._2, t._3))

    println(sorted.collect().toList)

  }
}

case class Girl(faceValue: Int, age: Int)

//按faceValue升序排序，如果相同则按age降序排序
object MyOrdering {
  implicit val girlOrdering: Ordering[Girl] = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue == y.faceValue) {
        y.age - x.age
      }
      else {
        x.faceValue - y.faceValue
      }
    }
  }
}