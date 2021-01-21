package ml.recommend

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 输入：item序列
  * 输出：item embedding 向量
  *
  * item2vec
  */
object Item2Vec {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("Item2Vec")
      .setMaster("local[*]")

    val spark = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()

    val samples = getSamples(spark)
    val model = trainItem2Vec(spark, samples)

    model.getVectors.show(false)
  }

  def trainItem2Vec(spark: SparkSession, samples: DataFrame) = {
    val word2vec: Word2Vec = new Word2Vec()
      .setVectorSize(3)
      .setWindowSize(3)
      .setMinCount(0)
      .setInputCol("itemList")

    word2vec.fit(samples)
  }

  def getSamples(spark: SparkSession) = {
    val rmDupEles = udf{ arr: Seq[String] =>
      val resArr = new ListBuffer[String]()
      var str = ""
      for (elem <- arr) {
        if (!elem.equals(str)){
          str = elem
          resArr.append(elem)
        }
      }
      resArr.toArray
    }

    import spark.implicits._

    val df: DataFrame = Seq(
      ("Bob", Array("F", "E", "C", "H")),
      ("Ali", Array("C", "A", "E", "B")),
      ("Tom", Array("D")),
      ("Zoe", Array("B", "B", "D"))
    ).toDF("userId", "itemList")

    df
      .withColumn("itemList", rmDupEles($"itemList"))
      .select($"itemList")
  }
}
