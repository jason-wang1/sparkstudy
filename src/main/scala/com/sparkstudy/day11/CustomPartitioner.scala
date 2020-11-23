package com.sparkstudy.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
  * Descreption:
  * Date: 2019年07月08日
  *
  * @author WangBo
  * @version 1.0
  */
object CustomPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CustomPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, Long)] = sc.parallelize(2 to 10).zipWithIndex()

    val func = (index: Int, it: Iterator[(Int, Long)]) => {
      it.map(x => "partid="+index+" value="+x)
    }

    //mapPartitionsWithIndex方法：通过原分区索引(Int)，把函数作用在每一个分区上
    val rdd1: RDD[String] = rdd.mapPartitionsWithIndex(func)
    println("自定义分区前的内容为：")
    rdd1.collect().toList.foreach(println)

    //使用自定义分区器分区
    val rdd2: RDD[(Int, Long)] = rdd.partitionBy(new CustomPartitioner(3))
    val rdd3: RDD[String] = rdd2.mapPartitionsWithIndex(func)
    println("自定义分区后的内容为：")
    rdd3.collect().toList.foreach(println)
  }

}

//自定义分区器
class CustomPartitioner(numPartition: Int) extends Partitioner {
  override def numPartitions: Int = numPartition

  //根据参数key返回分区号
  override def getPartition(key: Any): Int = {
    key.toString.toInt % numPartition
  }
}
