package ml.recommend

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

case class User2ItemScore(user: String, item: String, score: Double)
/**
  * 基于物品的协同过滤
  * 采用哈希表表示物品被评分的向量，以便做点积
  */
object ItemCF1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ItemCF")
      .setMaster("local[*]")
      .set("Spark.serializer", "org.apache.Spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(Array(
        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[Array[InternalRow]],
        classOf[UnsafeRow]
      ))

    val spark = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    // 计算向量的模
    val vecLen: UserDefinedFunction = udf { vector: Map[String, Double] =>
      var length = 0.0
      for (score <- vector.values) {
        length += (score * score)
      }
      Math.sqrt(length)
    }

    // 计算两个向量的余弦相似度
    val consinSim: UserDefinedFunction = udf {
      (vector1: Map[String, Double], vector2: Map[String, Double], vecLen1: Double, vecLen2: Double) =>
        var dot = 0.0
        for ((userId1, score1) <- vector1) {
          val score2: Double = vector2.getOrElse(userId1, 0)
          dot += (score1 * score2)
        }

        dot / (vecLen1 * vecLen2)
    }

    val df: DataFrame = Seq(
      ("Bob", "A", 5.0),
      ("Bob", "D", 1.0),
      ("Bob", "E", 1.0),
      ("Ali", "C", 1.0),
      ("Ali", "E", 4.0),
      ("Tom", "A", 4.0),
      ("Tom", "B", 2.0),
      ("Tom", "E", 2.0),
      ("Zoe", "A", 2.0),
      ("Zoe", "B", 5.0),
      ("Amy", "B", 1.0),
      ("Amy", "C", 4.0),
      ("Amy", "D", 4.0)
    ).toDF("userId", "itemId", "score")

    val scoreDF: DataFrame = df.rdd
      .map { case Row(userId: String, itemId: String, score: Double) => {
        (itemId, (userId, score))
      }}
      .groupByKey()
      .mapValues(iter => iter.toMap)
      .toDF("itemId", "scores")
      .withColumn("vecLen", vecLen($"scores"))

    scoreDF.show(false)


    val item2itemDF: DataFrame = scoreDF.alias("t1")
      .crossJoin(scoreDF.alias("t2")) // 笛卡尔积
      .where($"t1.itemId" < $"t2.itemId") // 保证不会重复地两两配对，后面不会重复计算

    item2itemDF.show(false)

    val itemSimDf: DataFrame = item2itemDF
      .withColumn("cosSim", consinSim($"t1.scores", $"t2.scores", $"t1.vecLen", $"t2.vecLen")) // 计算item之间的余弦相似度
      .select($"t1.itemId", $"t2.itemId", $"cosSim")

    itemSimDf
      .show(false)

  }
}
