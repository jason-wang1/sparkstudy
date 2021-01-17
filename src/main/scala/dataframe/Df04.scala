package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 输入：schema为（用户A，用户B）
  * 输出：schema为（用户A，用户B）
  *
  * 给一张用户A关注用户B的表，输出相互关注的用户对
  */
object Df04 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Df04").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = Seq(
      (12, 34),
      (12, 56),
      (12, 78),
      (34, 56),
      (34, 12)
    ).toDF("userA", "userB")

    // 找出相互关注的用户对
    df.alias("a").join(df.alias("b"), $"a.userA" === $"b.userB")
      .where($"a.userB" === $"b.userA")
      .select($"a.userA", $"a.userB")
      .show()
  }
}
