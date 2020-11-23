package com.review.teacher.KNN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}
import scala.math.{pow, sqrt}

// spark core实现KNN
// 优化后的算法：减少数据集的扫描
object KNNDemo4 {
  case class LabelPoint(point: Array[Double], label: String)

  def main(args: Array[String]): Unit = {
    val K = 11
    val conf = new SparkConf().setMaster("local[*]").setAppName("LogisticSGD")
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
    // RDD[样本数据的特征值, 待求分类点的特征值, 二者之间的距离, 样本数据的标签]
    // RDD[样本数据的特征值, 样本数据的标签, 待求分类点的特征值, 二者之间的距离]
    // RDD[lp, testDataValue每一个元素, 二者之间的距离]
    val distAndLabelRDD: RDD[(LabelPoint, Array[Double], Double)] = simpleRDD.flatMap(simpleLabelPoint => {
      val testDataValue: Array[LabelPoint] = testBC.value
      // lp, LabelPoint
      // testDataValue: Array[LabelPoint]    数组长度为9
      testDataValue.map(testLabelPoint => {
        val dist = getDistance(simpleLabelPoint.point, testLabelPoint.point)
        (simpleLabelPoint, testLabelPoint.point, dist)
      })
    })

    //3、计算 distsAndLabelRDD 中前K个点；
    // HashPartitioner cannot partition array keys.
    distAndLabelRDD.map{case (LabelPoint(point1, label), point2, dist) =>
      (point2.mkString, (dist, label))
    }.aggregateByKey(immutable.TreeSet[(Double, String)]())(
      // 分区内数据合并
      (bufferTreeSet: immutable.TreeSet[(Double, String)], elem: (Double, String)) => {
        val buffer = bufferTreeSet + elem
        buffer.take(K)
      },
      // 分区间数据合并
      (buffer1, buffer2) => (buffer1 ++  buffer2).take(K)
    ).mapValues(_.toList.map(_._2).groupBy(x=>x).mapValues(_.length).toList.sortBy(_._2).reverse)
      .foreach{case (key, value) => println(key, "|||", value)}

    sc.stop()
  }

  def getDistance(x: Array[Double], y: Array[Double]): Double ={
    sqrt(x.zip(y).map(elem => pow(elem._1 - elem._2, 2)).sum)
  }
}


// 4、计算K个点中类别的分类情况（K个点中类别做wordcount）
