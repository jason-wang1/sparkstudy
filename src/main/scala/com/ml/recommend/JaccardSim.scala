package com.ml.recommend

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Descreption: 计算杰卡德相似度
  * Date: 2020年12月20日
  *
  * @author WangBo
  * @version 1.0
  */
object JaccardSim {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df: DataFrame = Seq(
      ("u1", Array("a", "c", "d")),
      ("u2", Array("b", "d", "g", "f")),
      ("u3", Array("a", "c", "d")),
      ("u4", Array("c", "e", "g"))
    ).toDF("itemId", "labels")

    df.show()
    df.printSchema()
    
    val jaccardSim = udf{(labels1:Seq[String], labels2: Seq[String]) =>
      val totleLabels: Set[String] = labels1.toSet ++ labels2.toSet
      val intersect: Set[String] = labels1.toSet & labels2.toSet

      intersect.size / totleLabels.size.toDouble
    }

    val takeTopK: UserDefinedFunction = udf ({itemSims: Seq[Row] =>
      itemSims.take(2)}
    , ArrayType(StructType(StructField("sim", DoubleType) :: StructField("itemId", StringType) :: Nil)))

    df.alias("t1").crossJoin(df.alias("t2"))
      .where($"t1.itemId" =!= $"t2.itemId")
      .withColumn("sim", jaccardSim($"t1.labels", $"t2.labels"))
      .groupBy($"t1.itemId")
      .agg(sort_array(collect_list(struct($"sim", $"t2.itemId")), false).alias("simItems"))
      .withColumn("simItems", takeTopK($"simItems"))
      .show(false)
  }
}
