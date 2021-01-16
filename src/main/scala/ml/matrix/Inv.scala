package com.ml.matrix

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Rand

/**
  * Descreption: XXXX<br/>
  * Date: 2020年08月08日
  *
  * @author WangBo
  * @version 1.0
  */
object Inv {
  def main(args: Array[String]): Unit = {
//    val sparkConf: SparkConf = new SparkConf().setAppName("Inv").setMaster("local[*]")
//    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val matrix: DenseMatrix[Double] = new DenseMatrix[Double](3, 3, Array(1.0, 4.0, 3.0, 3.0, 6.0, 9.0, 6.0, 3.0, 5.0))
    println(inv(matrix))

  }
}
