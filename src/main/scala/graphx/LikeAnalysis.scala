package graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * 输入：用户关系图，包含用户ID、用户姓名、用户年龄、用户间点赞次数
  * 输出：
  *   1. vertices操作：年龄大于30的用户
  *   2. edges操作：点赞数大于5对用户ID对
  *   3. triplets操作，输出所有过点赞的用户对；输出点赞数大于5的用户对
  *   4. Degrees操作，输出最大出度用户ID及其度数、输出最大入度用户ID及其度数、输出最大度用户ID及其度数
  *   5. 转换操作：对每个用户年龄+10，对每个点赞数*2
  *   6. 结构操作：输出用户年龄大于30的子图
  *   7. Graph与VertexRDD的join操作，将每个用户的入度、出度作为用户节点的属性的一部分（图与度join）
  *   8. 聚合操作：对每一个用户，找出对自己点过赞的年龄最大对用户
  *   9. VertexRDD与VertexRDD的join操作，输出对自己点过赞的年龄最大对用户，如果没有被点过赞则说明没有
  */
object LikeAnalysis {

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("SNSAnalysisGraphX").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    //***************************************************************************************************
    //*******************************          图的属性          *****************************************
    //***************************************************************************************************
    println("**********************************************************")
    println("属性演示")
    println("**********************************************************")

    /** 1. vertices操作：年龄大于30的用户 */
    //方法一
    println("找出图中年龄大于30的顶点方法一：")

    /**
      * 其实这里还可以加入性别等信息，例如我们可以看年龄大于30岁且是female的人
      */
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
    //方法二
    println("找出图中年龄大于30的顶点方法二：")
    graph.vertices.filter(v => v._2._2 > 30).collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println

    /** 2. edges操作：点赞数大于5对用户ID对 */
    println("找出图中属性大于5的边：")
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println


    /** 3. triplets操作，输出所有过点赞的用户对；输出点赞数大于5的用户对 */
    //triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
    println("输出所有的tripltes：")
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    println("输出点赞数>5的tripltes：")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    /** 4. Degrees操作，输出最大出度用户ID及其度数、输出最大入度用户ID及其度数、输出最大度用户ID及其度数 */
    println("找出图中最大的出度、入度、度数：")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
    println

    //***************************************************************************************************
    //*******************************          转换操作          *****************************************
    //***************************************************************************************************
    println("**********************************************************")
    println("转换操作")
    println("**********************************************************")

    /** 5. 转换操作：对每个用户年龄+10，对每个点赞数*2 */
    println("顶点的转换操作，顶点age + 10：")
    graph.mapVertices { case (id, (name, age)) => (id, (name, age + 10)) }
      .vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2} 说明：${v._2._2._1} 分区？${v._2._2._2}"))

    println("\n边的转换操作，边的属性*2：")
    graph.mapEdges(e => e.attr * 2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //***************************************************************************************************
    //*******************************          结构操作          *****************************************
    //***************************************************************************************************
    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")

    /** 6. 结构操作：输出用户年龄大于30的子图 */
    println("顶点年纪>30的子图：")
    val subGraph: Graph[(String, PartitionID), PartitionID] = graph.subgraph(vpred = (id, vd) => vd._2 > 30)
    println("子图所有顶点：")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

    println("\n子图所有边：")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    //***************************************************************************************************
    //*******************************          join操作          *****************************************
    //***************************************************************************************************
    println("**********************************************************")
    println("join操作")
    println("**********************************************************")

    /** 7. Graph与VertexRDD的join操作，将每个用户的入度、出度作为用户节点的属性的一部分（图与度join） */
    val inDegrees: VertexRDD[Int] = graph.inDegrees

    // inDeg：入度，该用户被多少人关注；outDeg：出度，该用户关注了多少人
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }

    //新图initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
    val userGraph: Graph[User, PartitionID] = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }
    println("连接图的属性：")
    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))


    println("\n出度和入度相同的人员：")
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
    println

    //    ***************************************************************************************************
    //    *******************************          聚合操作   使用Spark 2.0.2版本的aggregateMessages       *****************************************
    //    ***************************************************************************************************
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")

    /** 8. 聚合操作：对每一个用户，找出对自己点过赞的年龄最大对用户 */
    println("找出对自己点过赞的年龄最大对用户：")
    val oldestFollower: VertexRDD[(String, Int)] = graph.aggregateMessages[(String, Int)](
      // 将源顶点的属性发送给目标顶点，map过程
      triplet => {

        // Map Function
        // Send message to destination vertex containing name and age
        //aggregateMessages[(String, Int)
        triplet.sendToDst(triplet.srcAttr._1, triplet.srcAttr._2)
      },
      // 得到最大追求者，reduce过程
      (a, b) => if (a._2 > b._2) a else b
    )

    /** 9. VertexRDD与VertexRDD的join操作，输出对自己点过赞的年龄最大对用户，如果没有被点过赞则说明没有 */
    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) => {
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }
    }

    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) => {
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }
    }.collect.foreach { case (id, str) => println(str) }
    println

    println("**********************************************************")
    println("找出年纪最小的追求者：")
    val youngestFollower: VertexRDD[(String, Int)] = graph.aggregateMessages[(String, Int)](
      // 将源顶点的属性发送给目标顶点，map过程
      triplet => {
        // Map Function
        // Send message to destination vertex containing name and age
        triplet.sendToDst(triplet.srcAttr._1, triplet.srcAttr._2)
      },
      // 得到最小 追求者，reduce过程
      (a, b) => if (a._2 > b._2) b else a
    )


    userGraph.vertices.leftJoin(youngestFollower) { (id, user, optYoungestFollower) =>
      optYoungestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the youngest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str) }
    println

    //    找出追求者的平均年纪
    val averageAgeaa: VertexRDD[(PartitionID, Double)] = graph.aggregateMessages[(Int, Double)](
      // 将源顶点的属性 (1, Age)发送给目标顶点，map过程
      triplet => {

        // Map Function
        // Send message to destination vertex containing name and age
        triplet.sendToDst((1, triplet.srcAttr._2.toDouble))
      },
      // 得到追求着的数量和总年龄
      (a, b) => ((a._1 + b._1), (a._2 + b._2))
    )


    println("找出追求者的平均年纪：")
    val averageAge: VertexRDD[Double] = graph.aggregateMessages[(Int, Double)](
      // 将源顶点的属性 (1, Age)发送给目标顶点，map过程
      triplet => {
        // Map Function
        // Send message to destination vertex containing name and age
        triplet.sendToDst((1, triplet.srcAttr._2.toDouble))
      },
      // 得到追求着的数量和总年龄
      (a, b) => ((a._1 + b._1), (a._2 + b._2))
    ).mapValues((id, p) => p._2 / p._1)

    userGraph.vertices.leftJoin(averageAge) { (id, user, optAverageAge) => {
      optAverageAge match {
        case None => s"${user.name} does not have any followers."
        case Some(avgAge) => s"The average age of ${user.name}\'s followers is $avgAge."
      }
    }
    }

    userGraph.vertices.leftJoin(averageAge) { (id, user, optAverageAge) => {
      val optAverageAgekk: Option[Double] = optAverageAge
      optAverageAge match {
        case None => s"${user.name} does not have any followers."
        case Some(avgAge) => s"The average age of ${user.name}\'s followers is $avgAge."
      }
    }
    }.collect.foreach { case (id, str) => println(str) }
    println

    //***************************************************************************************************
    //*******************************          实用操作          *****************************************
    //***************************************************************************************************
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出5到各顶点的最短：")
    val sourceId: VertexId = 5L
    // 定义源点

    val initialGraph: Graph[Double, PartitionID] = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)


    val sssp: Graph[Double, PartitionID] = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist),
      triplet => {
        // 计算权重
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      // mergeMsg: (A, A) => A)
      (a, b) => math.min(a, b) // 最短距离
    )
    println(sssp.vertices.collect.mkString("\n"))


  }
}
