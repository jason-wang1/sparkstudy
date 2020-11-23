package com.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月21日
  *
  * @author WangBo
  * @version 1.0
  */
object SqlPlan {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val t1: DataFrame = Seq(
      ("1001", "01", "78", 35),
      ("1003", "03", "65", 87)
    ).toDF("id", "cid", "did", "value")
    t1.createOrReplaceTempView("t1")

    val t2: DataFrame = Seq(
      ("1001", "01", "78", 35),
      ("1003", "03", "65", 87)
    ).toDF("id", "cid", "did", "value")
    t2.createOrReplaceTempView("t2")

//    spark.sql(
//      """
//        |SELECT sum(v)
//        |FROM (
//        |	SELECT t1.id
//        |		,1 + 2 + t1.value AS v
//        |	FROM t1
//        |	JOIN t2
//        |	WHERE t1.id = t2.id
//        |		AND t1.cid = 1
//        |		AND t1.did = t1.cid + 1
//        |		AND t2.id > 5
//        |	) iteblog
//        |
//      """.stripMargin)
//      .explain(true)

    val sales: DataFrame = Seq(
      ("1001", "01", 35),
      ("1003", "03", 87)
    ).toDF("id", "cid", "price")
    sales.createOrReplaceTempView("t1")

    val date: DataFrame = Seq(
      ("01", 12),
      ("02", 15),
      ("03", 18)
    ).toDF("cid", "did")
    date.createOrReplaceTempView("t2")

    spark.sql(
      """
        |select *
        |from t1
        |join t2
        |on t1.cid = t2.cid
        |where t1.cid = '01'
      """.stripMargin)
      .explain(true)

  }
}
