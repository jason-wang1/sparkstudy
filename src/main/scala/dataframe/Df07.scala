package dataframe

import org.apache.spark.sql.DataFrame
import ulits.SparkConfig

/**
  * 输入：schema为（商户id, 交易金额, 交易日期）
  * 输出：schema为（商户id, 最近2天的交易金额, 最近3天的交易金额, 交易日期）
  *
  * 输出每一天每个商户在最近2天、3天的交易金额
  */
object Df07 {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConfig("Df07").getSparkSession
    import spark.implicits._

    val df: DataFrame = Seq(
      ("m01", 3, "2017/1/21"),
      ("m02", 5, "2017/1/21"),
      ("m01", 4, "2017/1/21"),
      ("m01", 6, "2017/1/22"),
      ("m03", 1, "2017/1/23"),
      ("m03", 5, "2017/2/23"),
      ("m02", 2, "2017/1/23"),
      ("m01", 5, "2017/1/24"),
      ("m02", 3, "2017/1/25"),
      ("m03", 2, "2017/1/25"),
      ("m01", 1, "2017/1/26"),
      ("m02", 6, "2017/2/26")
    ).toDF("mid", "amt", "date")

    df.createOrReplaceTempView("trans")

    spark.sql(
      """
        |select *,
        |sum(amt) over(partition by mid order by date rows between 2 preceding and current row) sum_2
        |sum(amt) over(partition by mid order by date rows between 3 preceding and current row) sum_3
        |from (
        |   select mid, date, sum(amt) amt
        |   from trans
        |   group by mid, date) aa
        |order by date, mid
        |""".stripMargin).show(false)


  }
}
