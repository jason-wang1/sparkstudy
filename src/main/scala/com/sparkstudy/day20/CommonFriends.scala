package com.sparkstudy.day20

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年08月02日
  *
  * @author WangBo
  * @version 1.0
  */
object CommonFriends {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(s"${this.getClass.getName}")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

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
    vertexRDD

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
    edgeRDD

    val value: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    val graph = value

    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.collect().toList.foreach(println)

    println("**********")

    vertices.join(vertexRDD).collect().toList.foreach(println)

  }

}
