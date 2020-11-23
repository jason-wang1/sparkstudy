package com.sparkstudy.day14

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Descreption:使用UDFA操作DataFrame
  * 需求：统计员工平均薪资
  * Date: 2019年07月12日
  *
  * @author WangBo
  * @version 1.0
  */
class UDAFDemo1 extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(StructField("salary", DoubleType, true)))

  override def bufferSchema: StructType =
    StructType(StructField("sum", DoubleType)::StructField("count", DoubleType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //员工总薪资
    buffer(0) = 0.0
    buffer(1) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}

object UDAFDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDFDemo1")
      .master("local")
      .getOrCreate()

    val df: DataFrame = spark.read.json("D://data/employees.json")


  }

}
