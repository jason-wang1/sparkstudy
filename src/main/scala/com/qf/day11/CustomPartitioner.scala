package com.qf.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区器
  */
class CustomPartitioner(numPartition: Int) extends Partitioner{
  // 返回分区数
  override def numPartitions: Int = numPartition
  // 根据传入的key返回分区号
  override def getPartition(key: Any): Int = {
    key.toString.toInt % numPartition
  }
}
object CustomPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomPartitioner").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(0 to 10).zipWithIndex()
    println(rdd.collect.toBuffer)

    val func = (index: Int, it: Iterator[(Int, Long)]) => {
      it.map(x => "partid:" + index + ", value:" + x)
    }

    val rdd1: RDD[String] = rdd.mapPartitionsWithIndex(func)
    rdd1.collect.foreach(println)

    // 指定自定义分区器
    val rdd2 = rdd.partitionBy(new CustomPartitioner(3))
    println(rdd2.partitions.length)

    sc.stop()
  }
}
