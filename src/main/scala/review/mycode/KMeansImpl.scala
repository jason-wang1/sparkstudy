package com.review.mycode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.math._

/**
  * Descreption:
  * kmeans实现步骤：
  * 1、读数据、封装数据
  * 2、随机选择K个点
  * 3、计算所有点到K个点的距离
  * 4、遍历所有点，对每个点找距离最小的点，离谁最近就属于哪个分类
  * 5、计算K个分类的中心点
  * 6、计算新旧中心是否发生移动
  * 7、没有移动结束循环，否则转步骤3
  *
  * Date: 2019年09月05日
  *
  * @author WangBo
  * @version 1.0
  */
object KMeansImpl {

  case class labelPoint(label: String, point: Array[Double])
  case class pointWithIndex(point: Array[Double], index: Int)

  //计算两点间的距离
  def getDistance(point1: Array[Double], point2: Array[Double]): Double = {
    sqrt(point1.zip(point2).map(tup => pow(tup._1 - tup._2, 2)).sum)
  }


  /**
    * @param point 待聚类的一个点
    * @param centerPoints 一组中心点
    * @return 距离待聚类点最近的中心点的索引
    */
  def getPointWithCenterPoint(point: Array[Double], centerPoints: Array[pointWithIndex]): Int = {
    val distanceAndCenterPoint: Array[(Double, Int)] = centerPoints.map(item => {
      //(中心点与待聚类点的距离，中心点在数组中的索引)
      (getDistance(item.point, point), item.index)
    })

    distanceAndCenterPoint.sortBy(_._1).map(_._2).head
  }


  //两点累加
  def arrAdd(point1: Array[Double], point2: Array[Double]): Array[Double] = {
    point1.zip(point2).map(tup => {tup._1 + tup._2})
  }

  def main(args: Array[String]): Unit = {

    val K = 3
    val miniDist = 0.001

    val conf: SparkConf = new SparkConf().setAppName("KMeansImpl").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //1、读数据、封装数据
    val dataRDD: RDD[Array[String]] = sc.textFile("C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\data\\iris1.dat")
        .map(_.split(","))
        .filter(_.length==5)

    val labelPointRDD: RDD[labelPoint] = dataRDD.map(item => {
      labelPoint(item.last, item.init.map(_.toDouble))
    })

    //2、随机选择K个点作为中心点
    val centerPoints: Array[pointWithIndex] =
      labelPointRDD.takeSample(false, K).map(_.point)
        .zipWithIndex.map(tup => pointWithIndex(tup._1, tup._2))

    println("===== 最初的中心点 ======")
    centerPoints.map(tup => (tup.point.toList, tup.index)).foreach(println)

    var tempDist = 1.0

    while (tempDist > miniDist) {

      //3、计算所有点到K个点的距离
      //4、遍历所有点，对每个点找距离最小的点，离谁最近就属于哪个分类
      val pointWithCenterPoint: RDD[(Int, (Array[Double], Int))] = labelPointRDD.map(item => {
        //(距待聚类点最近的中心点的索引，(待聚类的点，1))
        (getPointWithCenterPoint(item.point, centerPoints), (item.point, 1))
      })


      //5、计算K个分类的新的中心点
      val newCenterPoints: Array[pointWithIndex] = pointWithCenterPoint.reduceByKey((x, y) => {
        //(累加后的点，累加的次数)
        (arrAdd(x._1, y._1), x._2 + y._2)
      })
        //(旧中心点索引，(累加后的点，累加次数))
        .map { case (oldCenterPointIndex: Int, (arrAdded: Array[Double], arrAddedNum: Int)) => {

        (pointWithIndex(arrAdded.map(_ / arrAddedNum), oldCenterPointIndex))
      }
      }.collect()


      //6、计算新旧中心移动距离
      val distances: immutable.IndexedSeq[Double] = for (i <- 0 until K) yield {
        getDistance(newCenterPoints.filter(_.index==i).head.point, centerPoints.filter(_.index==i).head.point)
      }

      val distance: Double = distances.sum

      println("新旧中心点移动距离为："+distance)

      //7、没有移动结束循环，否则转步骤3
      tempDist = distance

      //更新中心点
      for (item <- newCenterPoints) {
        centerPoints(item.index) = item
      }

    }

    println("final centerPoints:")
    centerPoints.map(_.point.toList).foreach(println)

    val pointWithCenterPoint: RDD[(Int, labelPoint)] = labelPointRDD.map(item => {
      //(距待聚类点最近的中心点的索引，(待聚类的点，1))
      (getPointWithCenterPoint(item.point, centerPoints), item)
    })

    println("最终聚类为：")
    pointWithCenterPoint.map(item => (item._1, (item._2.point.toList, item._2.label))).sortByKey().foreach(println)


    sc.stop()

  }

}
