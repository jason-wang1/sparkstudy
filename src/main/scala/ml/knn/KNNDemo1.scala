package ml.knn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{pow, sqrt}

// spark core实现KNN
object KNNDemo1 {
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
    simpleRDD.cache()
    val testData = dataRDD.filter(_.label=="").collect()

    // 2、计算 testRDD 中每个点与 simpleRDD 中每个点的距离，得到数据集T3
    // 坑：RDD 不能嵌套
    testData.map(lp1 => {
      // (距离，label)
      val distanceAndLabelRDD: RDD[(Double, String)] = simpleRDD.map(lp2 => (getDistance(lp1.point, lp2.point), lp2.label))
      //3、计算T3中前K个点；4、计算K个点中类别的分类情况（K个点中类别做wordcount）
      distanceAndLabelRDD.sortBy(_._1).take(K).map(_._2).groupBy(x=>x).mapValues(_.length).toList.sortBy(_._2).reverse.foreach(print)
      println("*******************************************************")
    })

    sc.stop()
  }

  def getDistance(x: Array[Double], y: Array[Double]): Double ={
    sqrt(x.zip(y).map(elem => pow(elem._1 - elem._2, 2)).sum)
  }
}
