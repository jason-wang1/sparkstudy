package com.review.mycode

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.math._

/**
  * Descreption:
  * 优化：使用aggregateByKey代替groupByKey
  *
  * Date: 2019年09月03日
  *
  * @author WangBo
  * @version 1.0
  */
object KNNDemo2 {

  case class LabelPoint(point: Array[Double], label: String)

  def getDistance(testPoint: Array[Double], samplePoint: Array[Double]): Double = {
    sqrt(testPoint.zip(samplePoint).map(tup => {
      pow(tup._1 - tup._2, 2)
    }).sum)
  }

  def main(args: Array[String]): Unit = {
    val start: Long = System.currentTimeMillis()

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KNN")
    val sc = new SparkContext(conf)
    val K = 11

    //1.读数据集，封装数据
    val dataRDD: RDD[LabelPoint] = sc.textFile("C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\data\\iris1.dat")
      .map(line => {
        val arr: Array[String] = line.split(",")
        if (arr.length == 5) {
          LabelPoint(arr.init.map(_.toDouble), arr.last)
        }
        else {
          LabelPoint(arr.map(_.toDouble), "")
        }
      })

    val testRDD: RDD[LabelPoint] = dataRDD.filter(_.label=="")    //测试数据
    val sampleRDD: RDD[LabelPoint] = dataRDD.filter(_.label!="")  //样本数据

    val testPoint: Array[Array[Double]] = testRDD.map(_.point).collect()  //测试数据的特征
    val testPointBC: Broadcast[Array[Array[Double]]] = sc.broadcast(testPoint)

//    dataRDD.map(ele => {
//      (ele.point.toList, ele.label)
//    }).foreach(println)

    //2.计算testRDD与sampleRDD每两点间的距离，得到是聚集T3
    //RDD[样本数据的label，测试数据的特征值，两数据的距离]
    val distanceAndLabel: RDD[(String, Array[Double], Double)] = sampleRDD.flatMap(sampleLabelPoint => {
      val samplePoint: Array[Double] = sampleLabelPoint.point
      val sampleLabel: String = sampleLabelPoint.label
      val testPointBCValue: Array[Array[Double]] = testPointBC.value
      testPointBCValue.map(testPoint => {
        val distance: Double = getDistance(testPoint, samplePoint)
        (sampleLabel, testPoint, distance)
      })
    })



    //3.按testRDD分类，取最近的前K个sample点
    //(待预测点特征，TreeSet[(距离，样本点特征)])
    val distanceSorted: RDD[(String, mutable.TreeSet[(Double, String)])] = distanceAndLabel.map { case (sampleLabel, testPoint, distance) =>
      //(testPoint, (distance, sampleLabel))
      (testPoint.mkString(","), (distance, sampleLabel))
    }.aggregateByKey(mutable.TreeSet[(Double, String)]())(
      //分区内数据合并
      (bufferTreeSet: mutable.TreeSet[(Double, String)], elem: (Double, String)) => {
        val buffer: mutable.TreeSet[(Double, String)] = bufferTreeSet + elem
        buffer.take(K)
      },
      //分区间数据合并
      (buffer1, buffer2) => (buffer1 ++ buffer2).take(K)
    )


    //4.对这些sample的label做wordcount
    val labelCounted: RDD[(String, List[(String, Int)])] =
      distanceSorted.mapValues(_.toList.map(_._2).groupBy(x=>x).mapValues(_.length).toList.sortBy(_._2).reverse)

    labelCounted.foreach(println)


    val end: Long = System.currentTimeMillis()
    val diff: Long = end - start

    println("程序所用时间："+diff)

  }

}

//1.读数据集，封装数据
//2.计算testRDD与sampleRDD每两点键的距离，得到是聚集T3
//3.按testRDD分类，取最近的前K个sample点
//4.对这些sample的label做wordcount

