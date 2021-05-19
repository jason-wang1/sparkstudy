package ml.recommend

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.control.Breaks

/**
  * 基于物品的协同过滤
  *
  * 优化：
  * 1. 采用有序数组保存稀疏向量，笛卡尔积后的dataframe空间大约可以减半，2n^2 -> n^2
  * 2. 时间复杂度增高，需要对n个向量排序
  * 3. 实际生产中uid为长度为10几的字符串，可以使用Long重新编码表示，空间可以减少到原来的1/10
  *    并且可以减少排序的复杂度，因为数值的大小比较比字符串更快
  */

object ItemCF2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ItemCfList")
      .setMaster("local[*]")

    val spark = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    // 计算向量的模
    val vecLen: UserDefinedFunction = udf { vector: Seq[Row] =>
      var length = 0.0
      for (score <- vector) {
        length += (score.getDouble(1) * score.getDouble(1))
      }
      Math.sqrt(length)
    }

    // 计算两个向量的余弦相似度
    val consinSim: UserDefinedFunction = udf {
      (vector1: Seq[Row], vector2: Seq[Row], vecLen1: Double, vecLen2: Double) =>
        var dot = 0.0
        var i = 0
        var j = 0
        val breaks = new Breaks
        while (i < vector1.size && j < vector2.size){
          breaks.breakable{
            if (vector1(i).getLong(0) < vector2(j).getLong(0)) {
              i += 1
              breaks.break()
            } else if (vector1(i).getLong(0) > vector2(j).getLong(0)){
              j += 1
              breaks.break()
            } else {
              dot += vector1(i).getDouble(1) * vector2(j).getDouble(1)
              i += 1
              j += 1
            }
          }
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

    val userCodedDf: DataFrame = df.select($"userId").distinct()
      .withColumn("uid", monotonically_increasing_id)
//      .withColumn("uid", dense_rank().over(Window.orderBy($"userId")))

    userCodedDf.show(false)

    val scoreDF = df.join(userCodedDf, "userId")
      .groupBy($"itemId")
      .agg(sort_array(collect_list(struct($"uid", $"score"))).alias("scores"))
      .withColumn("vecLen", vecLen($"scores"))
      .cache()

    scoreDF.show(false)
    scoreDF.printSchema()

    val item2itemDF: DataFrame = scoreDF.alias("t1")
      .crossJoin(scoreDF.alias("t2")) // 笛卡尔积
      .where($"t1.itemId" < $"t2.itemId") // 保证不会重复地两两配对，后面不会重复计算

    item2itemDF.show(false)

    val itemSimDf: DataFrame = item2itemDF
      .withColumn("cosSim", consinSim($"t1.scores", $"t2.scores", $"t1.vecLen", $"t2.vecLen")) // 计算item之间的余弦相似度
      .select($"t1.itemId", $"t2.itemId", $"cosSim")

    itemSimDf.show(false)

  }
}
