package com.graphx
import org.apache.spark.graphx._
import ulits.SparkConfig
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
/**
  * 输入：用户关系图，包含用户ID、用户姓名、用户职业、用户间的关系
  * 输出：1. 职业为博士后的用户数；2. 边中源 ID 大于目标 ID 的数量；遍历用户间的关系（含职业）
  *
  * 考察点：Graph的三大属性：vertices、edges、triplets
  */
object GraphAttributes {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConfig("GraphDemo").getSparkContext

    // 创建用户顶点
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // 创建用户关系的边
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // 创建默认用户节点，以防存在缺失了用户的关系
    val defaultUser = ("John Doe", "Missing")

    // 创建图
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    // 计算图中职业是博士后的节点数量
    val postDocCount: VertexId = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println(s"职业为博士后的节点数量为：$postDocCount")

    // 统计边中源 ID 大于目标 ID 的数量
    val srcLagerCount: VertexId = graph.edges.filter(e => e.srcId > e.dstId).count
    println(s"源ID大于目标ID的数量是：$srcLagerCount")

    // 查看用户之间的关系
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
  }
}
