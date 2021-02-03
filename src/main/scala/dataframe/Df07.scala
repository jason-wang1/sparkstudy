package dataframe

import org.apache.spark.sql.DataFrame
import ulits.SparkConfig

/**
  * 输入：schema为（商户id, 交易金额, 交易日期）
  * 输出：schema为（商户id, 最近2天的交易总金额, 最近3天的交易总金额, 交易日期）
  *
  * 输出每一天每个商户在最近2天的交易总金额、最近3天的交易总金额
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
      ("m03", 5, "2017/1/23"),
      ("m02", 2, "2017/1/23"),
      ("m01", 5, "2017/1/24"),
      ("m02", 3, "2017/1/25"),
      ("m03", 2, "2017/1/25"),
      ("m01", 1, "2017/1/26"),
      ("m04", 6, "2017/1/26")
    ).toDF("mid", "amt", "date")

    df.createOrReplaceTempView("trans")

    spark.sql(
      """
        |select aaa.mid, aaa.date,
        |sum(aaa.amt) over(partition by aaa.mid order by aaa.date rows between 1 preceding and current row) sum_amt_2,
        |sum(aaa.amt) over(partition by aaa.mid order by aaa.date rows between 2 preceding and current row) sum_amt_3
        |from (
        | select aa.mid, aa.date, sum(nvl(bb.amt, 0)) amt
        | from (
        |   select a.mid, b.date
        |   from (
        |     select distinct mid
        |     from trans
        |   ) a
        |   cross join (
        |     select distinct date
        |     from trans
        |   ) b
        | ) aa
        | left join
        | trans bb
        | on aa.mid = bb.mid
        | and aa.date = bb.date
        | group by aa.date, aa.mid
        |) aaa
        |order by aaa.date, aaa.mid
        |""".stripMargin).show(100, truncate = false)


  }
}
