package com.review.teacher.KNN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.math.{pow, sqrt}

// spark core实现KNN
// 优化后的算法：减少数据集的扫描
object KNNDemo2 {
  case class LabelPoint(point: Array[Double], label: String)

  def main(args: Array[String]): Unit = {
    val K = 11
    val conf = new SparkConf().setMaster("local").setAppName("LogisticSGD")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    // 1、读数据集，封装数据；分为：样本数据集(simpleRDD)；测试数据集(testRDD)
    // 坑：如何封装数据集
    val dataRDD: RDD[LabelPoint] = sc.textFile("./src/main/data/iris1.dat")
      .map(line => {
        val arr = line.split(",")
        if (arr.length == 5)
          LabelPoint(arr.init.map(_.toDouble), arr.last)
        else
          LabelPoint(arr.map(_.toDouble), "")
      })
    val simpleRDD = dataRDD.filter(_.label!="")
    val points: Array[LabelPoint] = dataRDD.filter(_.label == "").collect()
    val testData = points
    val testBC = sc.broadcast(testData)

    // 2、计算 simpleRDD 中每个点与 testBC 中每个点的距离
    // (key, 距离，label)
//    val distsAndLabelRDD: RDD[(String, (Double, String))] = simpleRDD.flatMap(lp1 => {
//      val testArr: Array[Array[Double]] = testBC.value.map(_.point)
//      val pointAndDists: Array[(String, Double)] = getPointsDistance(lp1.point, testArr)
//      val label: String = lp1.label
//      val result: mutable.ArraySeq[(String, (Double, String))] = pointAndDists.map { case (key, dists) => (key, (dists, label)) }
//      result
//    })
//    distsAndLabelRDD.foreach(println)
    //RDD[(String, (Double, String))]

    //3、计算 distsAndLabelRDD 中前K个点；

    // 4、计算K个点中类别的分类情况（K个点中类别做wordcount）

    sc.stop()
  }

  def getDistance(x: Array[Double], y: Array[Double]): Double ={
    sqrt(x.zip(y).map(elem => pow(elem._1 - elem._2, 2)).sum)
  }

  // 返回Array(key， 距离)
  def getPointsDistance(x: Array[Double], points: Array[Array[Double]]): Array[(String, Double)] ={
    points.map(y => (y.mkString(","), getDistance(x,y)))
  }
}