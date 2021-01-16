package com.review.teacher.KMeans

import scala.math.{pow, sqrt}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  * kmeans实现步骤：
  * 1、读数据、封装数据
  * 2、随机选择K个点
  * 3、计算所有点到K个点的距离
  * 4、遍历所有点，对每个点找距离最小的点，离谁最近就属于哪个分类
  * 5、计算K个分类的中心点
  * 6、计算新旧中心是否发生移动
  * 7、没有移动结束循环，否则转步骤3
  */
object KMeansImpl {
  case class LablePoint(label: String, point: Array[Double])

  def main(args: Array[String]) {
    // K : 分类的个数；minDist : 中心点移动的最小距离，迭代终止条件；两个参数最好从命令行传进来
    val K = 3
    val minDist = 0.001

    val conf = new SparkConf().setMaster("local[*]").setAppName("KMeans")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 1、读文件，封装数据
    val lines = sc.textFile("C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\data\\iris1.dat")
    val data: RDD[LablePoint] = lines.filter(_.trim.size != 0)
      .map(line => {
        val arr = line.split(",")
        LablePoint(arr.last, arr.init.map(_.toDouble))
      })
    data.cache()

    // 2、获得K个随机的中心点
    val centerPoints: Array[Array[Double]] = data.takeSample(withReplacement = false, K).map(_.point)
    var tempDist = 1.0

    while(tempDist > minDist) {
      // 3、计算所有点到K个点的距离；
      // 得到每个点的分类 [分类编号, (特征, 1.0)]；1.0 在后面计算中心点时用于计数
      //(最短距离， (点的特征，1.0))
      val indexRDD: RDD[(Int, (Array[Double], Double))] = data.map(p => (getIndex(p.point, centerPoints), (p.point, 1.0)))

      // 计算新的中心点
      def arrayAdd(x: Array[Double], y: Array[Double]): Array[Double] = x.zip(y).map(elem => elem._1 + elem._2)

      // 将所用的点按照计算出的分类计算
      val catalogRDD: RDD[(Int, (Array[Double], Double))] = indexRDD.reduceByKey((x, y) =>
        (arrayAdd(x._1, y._1), x._2 + y._2)
      )

      // 计算新的中心点
      val newCenterPoints: collection.Map[Int, Array[Double]] =
        catalogRDD.map{ case (index, (point, count)) => (index, point.map(_ / count)) }
          .collectAsMap()

      // 计算中心点移动的距离
      val dist: immutable.IndexedSeq[Double] = for (i <- 0 until K) yield {
        getDistance(centerPoints(i), newCenterPoints(i))
      }

      tempDist = dist.sum

      // 重新定义中心点
      for ((key, value) <- newCenterPoints) {
        centerPoints(key) = value
      }
      println("distSum = " + tempDist + "")
    }

    // 打印结果
    println("Final centers:")
    centerPoints.foreach(x => println(x.toBuffer))

    sc.stop()
  }

  private def getDistance(x: Array[Double], y: Array[Double]): Double = {
    sqrt(x.zip(y).map(elem => pow(elem._1 - elem._2, 2)).sum)
  }

  /**
    * @param p 一个点
    * @param centers 一群中心点
    * @return 这个点p距离这群中心点中最近中心点的距离
    */
  private def getIndex(p: Array[Double], centers: Array[Array[Double]]): Int = {
    val dist: Array[Double] = centers.map(point => getDistance(point, p))
    dist.indexOf(dist.min)
  }
}