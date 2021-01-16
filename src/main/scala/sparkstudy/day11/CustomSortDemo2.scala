package com.sparkstudy.day11

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Descreption: 实现自定义排序2
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */
object CustomSortDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("customSort").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val girlInfo: RDD[(String, Int, Int)] = sc.parallelize(List(("mimi", 90, 32), ("bingbing", 85, 31), ("yuanyuan", 87, 33)))

    //第二种排序方式
    val sorted: RDD[(String, Int, Int)] = girlInfo.sortBy(t => Girl2(t._1, t._2, t._3))
    println(sorted.collect().toList)

    sc.stop()
  }

}

//第二种排序方式，按faceValue升序排序，如果相同则按age降序排序
case class Girl2(name: String, faceValue: Int, age: Int) extends Ordered[Girl2]{
  override def compare(that: Girl2): Int = {
    if (this.faceValue == that.faceValue) {
      that.age - this.age
    }else {
      this.faceValue - that.faceValue
    }
  }
}
