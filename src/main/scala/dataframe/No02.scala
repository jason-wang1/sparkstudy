package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月10日
  *
  * @author WangBo
  * @version 1.0
  */
object No02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("No01").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val frame: DataFrame = Seq(
      ("u01", "2017/1/21", 5),
      ("u02", "2017/1/23", 6),
      ("u03", "2017/1/22", 8),
      ("u04", "2017/1/20", 3),
      ("u01", "2017/1/23", 6),
      ("u01", "2017/2/21", 8),
      ("u02", "2017/1/23", 6),
      ("u01", "2017/2/22", 4)
    ).toDF("userId", "visitDate", "visitCount")

    val w: WindowSpec = Window.partitionBy($"userId").orderBy($"visitMonth").rangeBetween(Window.unboundedPreceding, Window.currentRow)

    frame
      .withColumn("visitMonth", $"visitDate".substr(0, 6))
      .groupBy($"userId", $"visitMonth").agg(sum($"visitCount").alias("visitCountMonth"))
      .select($"userId", $"visitMonth", $"visitCountMonth", sum($"visitCountMonth").over(w).alias("visitCountSum"))
      .show()

  }
}
