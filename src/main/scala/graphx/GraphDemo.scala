package com.graphx
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
/**
  * Descreption: XXXX<br/>
  * Date: 2020年04月13日
  *
  * @author WangBo
  * @version 1.0
  */
object GraphDemo {
  def main(args: Array[String]): Unit = {
    // Assume the SparkContext has already been constructed
    val sparkConf = new SparkConf().setAppName("graphDemo").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // 计算图中职业是博士后的节点数量
    val postDocCount: VertexId = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println(s"职业为博士后的节点数量为：$postDocCount")

    // 统计边中源 ID 大于目标 ID 的数量
    val srcLagerCount: VertexId = graph.edges.filter(e => e.srcId > e.dstId).count
    println(s"源ID大于目标ID的数量是：$srcLagerCount")

    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
  }
}
