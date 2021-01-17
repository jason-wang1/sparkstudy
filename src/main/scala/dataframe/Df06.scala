package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 输入：schema为（时间戳，用户id，商品id）
  * 输出：schema为（商品id，uv）
  *
  * 给一张用户点击商品明细表，输出uv大于1的top3的商品
  */
object Df06 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = Seq(
      ("1582044689", "001", "310"),
      ("1582044692", "002", "310"),
      ("1582044692", "002", "310"),
      ("1582044692", "002", "332"),
      ("1582044692", "002", "332"),
      ("1582044695", "001", "356"),
      ("1582044695", "001", "356"),
      ("1582044695", "001", "350")
      // 时间戳，用户id，商品id
    ).toDF("ts", "uid", "pid")


    // 求uv大于1的top3的商品
    df.groupBy($"pid").agg(countDistinct($"uid").alias("uv"))
      .where($"uv" > 1)
      .sort($"uv".desc)
      .limit(3)
      .show()
  }
}
