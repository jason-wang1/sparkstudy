package com.dataframe.sqlpractice2

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年07月02日
  *
  * @author WangBo
  * @version 1.0
  */
object No03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No02").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val frame: DataFrame = Seq(
      ("A", "2015-01", 5),
      ("A", "2015-01", 15),
      ("B", "2015-01", 5),
      ("A", "2015-01", 8),
      ("B", "2015-01", 25),
      ("A", "2015-01", 5),
      ("A", "2015-02", 4),
      ("A", "2015-02", 6),
      ("B", "2015-02", 10),
      ("B", "2015-02", 5)
    ).toDF("userid", "month", "visits")

    val w: WindowSpec = Window.partitionBy($"userid").orderBy($"month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    frame.groupBy($"userid", $"month").agg(sum($"visits").alias("visits"))
      .select($"userid", $"month",  max($"visits").over(w).alias("max"), sum($"visits").over(w).alias("sum"))
//      .show()

    frame.createOrReplaceTempView("v")

    spark.sql(
      """
        |select userid, month,
        |max(visits) over(partition by userid order by month rows between unbounded preceding and current row) as max,
        |sum(visits) over(partition by userid order by month rows between unbounded preceding and current row) as sum
        |from(
        |  select userid, month, sum(visits) as visits
        |  from v
        |  group by userid, month)
      """.stripMargin)
      .show()

  }
}
