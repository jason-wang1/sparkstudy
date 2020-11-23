package com.sparkstudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年09月24日
  *
  * @author WangBo
  * @version 1.0
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local")
    val sc = new SparkContext(conf)

//    val tuples: Array[(Int, Int)] = Array((1, 2), (3, 4))
    ////    val tuplesRDD: RDD[(Int, Int)] = sc.parallelize(tuples)
    ////
    //////    tuplesRDD.saveAsSequenceFile("D://test")
    ////
    ////    val result: RDD[(Int, Int)] = sc.sequenceFile[Int, Int]("D://test")
    ////    println(result.collect().toList)

    val rdd: RDD[Int] = sc.parallelize(Array(2, 3, 4, 5))
    rdd.saveAsObjectFile("D://test")

    val objFile: RDD[Int] = sc.objectFile[Int]("D://test")
  }

}
