package com.release

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 图计算例子
  *
  */
object CommonFriends {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
      //搞定第二个需求
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 构造出点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("阿灿", 50)),
      (2L, ("阿尤", 38)),
      (6L, ("天哥哥", 26)),
      (9L, ("阿豪", 46)),
      (16L, ("内马尔", 28)),
      (21L, ("马塞洛", 29)),
      (44L, ("C罗", 33)),
      (5L, ("梅西", 30)),
      (7L, ("法尔考", 56)),
      (133L, ("詹皇", 34)),
      (138L, ("韦德", 36)),
      (158L, ("大姚", 33))
    ))
    // 构造出边的集合
  val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(Seq(
    Edge(1L, 133L, 0),
    Edge(2L, 133L, 0),
    Edge(6L, 133L, 0),
    Edge(9L, 133L, 0),
    Edge(6L, 138L, 0),
    Edge(21L, 138L, 0),
    Edge(16L, 138L, 0),
    Edge(44L, 138L, 0),
    Edge(5L, 158L, 0),
    Edge(7L, 158L, 0)
  ))
    val value: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    // 调用图计算的构造方法
    val graph = value
    //图计算原理就是取出其中一个最小的值，为顶点。
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //用另一种形式画的新图，以最小的值为中心，其他值围绕着它
    //VertexId是2元元组，元素为新图的一个边上的两个元素
    //vertices.foreach(println)

    vertices.join(vertexRDD).map{
      case (userid, (cmid, (name, age))) =>(cmid,List((name,age)))
    }.reduceByKey(_++_).foreach(println)
    //userid与cmid为新图上一个边上的两个点,cmid为一张图位于中间的最小值的那个点
    //(1,List((内马尔,28), (C罗,33), (马塞洛,29), (詹皇,34), (阿灿,50), (阿豪,46), (韦德,36), (天哥哥,26), (阿尤,38)))
    //(5,List((梅西,30), (大姚,33), (法尔考,56)))

  }
}
