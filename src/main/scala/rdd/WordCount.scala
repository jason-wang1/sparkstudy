package rdd

import java.net.URL
import org.apache.spark.sql.{Dataset, SparkSession}
import ulits.SparkConfig

case class Words(word: String, count: Int)

/**
  * WordCount，并降序排序
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new SparkConfig("WordCount").getSparkSession
    import spark.implicits._

    val inputUrl: URL = this.getClass.getResource("/data/test.txt")
    val df: Dataset[String] = spark.read.textFile(inputUrl.getPath)
    df.rdd
      .flatMap(_.split("\\W+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map { case (key, value) => Words(key, value) }
      .toDS
      .show(100)
  }
}
