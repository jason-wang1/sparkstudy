package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 输入：schema为（店铺名称，访客id）
  * 输出：schema为（店铺名称，访客id，访问次数，排名）
  *
  * 给一张用户点击店铺的明细，输出点击每个店铺最多的top3用户，并给出该用户点击该店铺次数以及店铺内的排名
  */
object Df03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Df03").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df: DataFrame = Seq(
      ("u1", "a"),
      ("u2", "b"),
      ("u2", "b"),
      ("u1", "b"),
      ("u1", "b"),
      ("u1", "a"),
      ("u3", "c"),
      ("u4", "b"),
      ("u1", "a"),
      ("u2", "c"),
      ("u5", "b"),
      ("u4", "b"),
      ("u6", "c"),
      ("u2", "c"),
      ("u1", "b"),
      ("u2", "a"),
      ("u2", "a"),
      ("u2", "a"),
      ("u2", "a")
    ).toDF("userId", "shopId")

    val w: WindowSpec = Window.partitionBy($"shopId").orderBy($"visitCount".desc)
    df
      .groupBy($"userId", $"shopId").agg(count($"*").alias("visitCount"))
      .select($"*", rank().over(w).alias("rk"))
      .where($"rk" < 4)
      .show()
  }
}
