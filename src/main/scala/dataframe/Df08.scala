package dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import ulits.SparkConfig

/**
  * 输入：schema为（用户id, 当天最后一次交易后的余额（没有交易则为null）, 日期）
  * 输出：schema为（用户id, 当天时点余额, 日期）
  *
  * 通过交易日志输出用户每天时点余额
  */
object Df08 {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConfig("Df07").getSparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df: DataFrame = Seq(
      ("01", Some(3), 21),
      ("01", None, 22),
      ("01", None, 23),
      ("01", Some(6), 24),
      ("01", None, 25),
      ("01", Some(5), 26),
      ("08", None, 21),
      ("08", None, 22),
      ("08", Some(3), 23),
      ("08", None, 24),
      ("08", Some(1), 25),
      ("08", None, 26)
    ).toDF("uin", "amt", "dt").cache()

    df.show()

    val w1 = Window.partitionBy($"uin").orderBy($"dt").rangeBetween(Window.unboundedPreceding, Window.currentRow)
    val w2 = Window.partitionBy($"uin", $"sum")
    df.withColumn("sum", sum($"amt").over(w1))
      .withColumn("bal", max($"amt").over(w2))
      .select("uin", "amt", "bal", "dt")
      .show()
  }
}
