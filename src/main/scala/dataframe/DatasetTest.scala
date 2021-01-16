package com.dataframe

/**
  * Descreption: XXXX<br/>
  * Date: 2020年07月16日
  *
  * @author WangBo
  * @version 1.0
  */
import org.apache.spark.sql._

import scala.reflect.ClassTag

// case class 中的字段应该是spark提供的内置 encoder
case class MyObj(i: Int, u: String, s: Seq[String])
//class MyObj(val i: Int)

case class Wrap[T](unwrap: T)

object DatasetTest {
//  val dataList = List(
//    SimpleTuple(5, "abc"),
//    SimpleTuple(6, "bcd")
//  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()


    import spark.implicits._
    val d: DataFrame = spark.createDataset(Seq(
      MyObj(1, java.util.UUID.randomUUID.toString, Seq("foo", "scala", "foo")),
      MyObj(2, java.util.UUID.randomUUID.toString, Seq("bar"))
    )).toDF("i","u","s")

    d.printSchema()

    val ds = d.map((row: Row) => {
      val id: Int = row.getInt(0)
      val set: Seq[String] = row.getSeq(2).distinct
      (id, set)
    }).toDF("id", "set")
    ds.show()
  }
}
