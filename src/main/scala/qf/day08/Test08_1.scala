package com.qf.day08

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Test08_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10)

    /**
      * withReplacement: Boolean 表示抽出的数据是否放回，true为有放回的抽样
      * fraction: Double 抽样比例，比如0.3代表30%，该值不准确，有浮动
      * seed: Long = Utils.random.nextLong 指定随机数生成的种子，该参数默认不传
      * //      */
    //    val sample = rdd.sample(true, 0.5)
    //
    //    println(sample.collect.toBuffer)

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    rdd1.foreachPartition(x => println(x.reduce(_ + _)))

    val rdd2 = sc.parallelize(List(("tom", 2),("jerry", 3),("kitty", 1), ("jerry", 2), ("kitty", 3)), 2)
    println(rdd2.partitions.length)

    rdd2.repartitionAndSortWithinPartitions(new HashPartitioner(2)).foreach(println)

    sc.stop()
  }
}
