package com.review.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Question {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName(s"${this.getClass.getCanonicalName}")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val arr1 = ('A' to 'Z').toArray
    val arr2 = (1 to 26).toArray

    val rdd1 = sc.makeRDD(arr1)
    val rdd2 = sc.makeRDD(arr2)
    println(rdd1.getNumPartitions, rdd2.partitions.length)

    val rdd3: RDD[(Int, Char)] = rdd1.map(char => (char.toInt - 64, char))
    val rdd4: RDD[(Int, Int)] = rdd2.map(n => (n, n))

    val rdd5 = rdd3.join(rdd4, 2)
    println(rdd1.getNumPartitions, rdd2.partitions.length, rdd3.getNumPartitions, rdd4.getNumPartitions, rdd5.getNumPartitions)
    println(rdd1.partitioner, rdd2.partitioner, rdd3.partitioner, rdd4.partitioner, rdd5.partitioner)

    rdd5.foreach(println)

    val arr3 = (1 to 26).toArray
    val rdd6 = sc.makeRDD(arr3)
    val rdd7 = rdd6.repartition(6)
    rdd7.count

    Thread.sleep(10000000)
    sc.stop()
  }
}

// 问题1：rdd1.getNumPartitions = ?
// 问题2：rdd3.getNumPartitions = ?
// 问题3：rdd1.partitioner = ?
// 问题4：rdd3.partitioner = ?
// 问题5：rdd5.partitioner = ?

// 问题6：从 rdd1 到 rdd5 的DAG图怎么画
// 问题7：程序中一共有多少个Task？

// 问题8：如何让join后只有2个分区