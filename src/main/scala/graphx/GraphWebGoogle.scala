package graphx

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年04月14日
  *
  * @author WangBo
  * @version 1.0
  */
object GraphWebGoogle {
  def main(args: Array[String]): Unit = {
    // Assume the SparkContext has already been constructed
    val sparkConf = new SparkConf().setAppName("GraphWebGoogle").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)

    //数据存放的目录；
    var dataPath = "C:\\Users\\BoWANG\\IdeaProjects\\sparkstudy\\src\\main\\scala\\data\\"
    val graphFromFile: Graph[PartitionID, PartitionID] = GraphLoader.edgeListFile(sc, dataPath + "web-Google.txt", numEdgePartitions = 4)

//    //统计顶点的数量
//    println("graphFromFile.vertices.count：    " + graphFromFile.vertices.count())
//    //统计边的数量
//    println("graphFromFile.edges.count：    " + graphFromFile.edges.count())

//    //顶点属性操作
//    val subGraph: Graph[PartitionID, PartitionID] =graphFromFile.subgraph(epred = e  => e.srcId > e.dstId)
//    for(elem <- subGraph.vertices.take(10)) {
//      println("subGraph.vertices：    "+  elem)
//    }
//    val tempGraph: Graph[PartitionID, PartitionID] = graphFromFile.mapVertices((vid, attr) => attr.toInt*2)
//    for(elem <- tempGraph.vertices.take(10)) {
//      println("subGraph.vertices：    "+  elem)
//    }

//    //构建子图
    //    val subGraph: Graph[PartitionID, PartitionID] =graphFromFile.subgraph(
    //        epred = e  => e.srcId > e.dstId,
    //        (vid, attr) => vid < 50
    //      )
    //    for(elem <- subGraph.edges.take(10)) {
    //      println("subGraph.edges：    "+  elem)
    //    }
    //    println("subGraph.vertices.count()：    "+  subGraph.vertices.count())
    //    println("subGraph.edges.count()：    "+  subGraph.edges.count())

//    //计算入度，出度
//    val tmp: VertexRDD[PartitionID] =graphFromFile.inDegrees
//    for (elem <- tmp.take(10)) {
//      println("graphFromFile.inDegrees：    " + elem)
//    }
//    val tmp1: VertexRDD[PartitionID] =graphFromFile.outDegrees
//    for (elem <- tmp1.take(10)) {
//      println("graphFromFile.outDegrees：    " + elem)
//    }

//    //计算度最大的顶点
//    def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int) = if(a._2 > b._2) a else b
//    println("graphFromFile.degrees.reduce(max)：    " + graphFromFile.degrees.reduce(max) )
//    println("graphFromFile.inDegrees.reduce(max)：    " + graphFromFile.inDegrees.reduce(max) )
//    println("graphFromFile.outDegrees.reduce(max)：    " + graphFromFile.outDegrees.reduce(max) )

//    val rawGraph: Graph[PartitionID, PartitionID] =graphFromFile.mapVertices((id, attr) =>0 ) //把顶点属性设置为0
//    for (elem <- rawGraph.vertices.take(10)) {
//      println("rawGraph.vertices：    " + elem)
//    }
//
//    val outDeg: VertexRDD[PartitionID] =rawGraph.outDegrees //找到所有outDegrees 的顶点的集合
//    val tmpJoinVertices: Graph[PartitionID, PartitionID] =rawGraph.joinVertices[Int](outDeg)((_, _, optDeg) => optDeg)
//    for (elem <- tmpJoinVertices.vertices.take(10)) {
//      println("tmpJoinVertices.vertices：    " + elem)
//    }
//    val tmpouterJoinVertices: Graph[PartitionID, PartitionID] =rawGraph.outerJoinVertices[Int,Int](outDeg)((_, _, optDeg) =>optDeg.getOrElse(0))
//    for (elem <- tmpouterJoinVertices.vertices.take(10)) {
//      println("tmpouterJoinVertices.vertices：    " + elem)
//    }

//    //创建一个用户关注图，顶点id为用户年龄
//    val graph: Graph[Double, PartitionID] = GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)
//    for (elem <- graph.vertices.take(10)) {
//      println(s"graph.vertices:  $elem")
//    }
//    for (elem <- graph.edges.take(10)) {
//      println(s"graph.edges:  $elem")
//    }
//    //计算出比这个用户年龄大的用户的个数以及比这个用户年龄大的用户的总年龄
//    val olderFollowers: VertexRDD[(PartitionID, Double)] = graph.aggregateMessages[(PartitionID, Double)](
//      triplet => {
//        if (triplet.srcAttr > triplet.dstAttr) {
//          //发送消息到目标顶点包含计数和年龄
//          triplet.sendToDst((1, triplet.srcAttr))
//        }
//      },
//      //计数和年龄分别相加
//      (a, b) => (a._1 + b._1, a._2 + b._2) //Reduce 函数
//    )
//    //获得比这个用户年龄大的边随者的平均年龄
//    val avgAgeOfOlderFollowers: VertexRDD[Double] = olderFollowers.mapValues((id, value) => {
//      value match {
//        case (count, totleAge) => totleAge / count
//        case _ => 0L
//      }
//    })
//    avgAgeOfOlderFollowers.collect.foreach(println(_))

//    /** Pregel：计算任何网页0与其他网页之间的最短路径 */
    //    val sourceId: VertexId = 0
    //    val g: Graph[Double, PartitionID] = graphFromFile.mapVertices((id, _) =>
    //      if (id == sourceId) 0.0 else Double.PositiveInfinity
    //    )
    //
    //    val sssp: Graph[Double, PartitionID] = g.pregel(Double.PositiveInfinity)(
    //      (id, dist, newDist) => math.min(dist, newDist), //顶点程序
    //      triplet => { //发送消息
    //        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
    //          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    //        } else {
    //          Iterator.empty
    //        }
    //      },
    //      (a, b) => math.min(a, b) //合并消息
    //    )
    //    println(sssp.vertices.collect.mkString("\n"))

//    /** PageRank */
//    val rank: VertexRDD[Double] = graphFromFile.pageRank(0.01).vertices
//    for (elem <- rank.take(10)) {
//      println("rank：    " + elem)
//    }

    /** TriangleCount */
    val graphFortriangleCount: Graph[PartitionID, PartitionID] = GraphLoader.edgeListFile(sc, dataPath + "web-Google.txt", true)
    val c: VertexRDD[PartitionID] = graphFromFile.triangleCount().vertices
    for (elem <- c.take(10)) {
      println("triangleCount：    " + elem)
    }

    //    while (true) {}
    sc.stop()
  }
}
