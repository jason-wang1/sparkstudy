package com.qf.day14

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/**
  * 使用UDAF操作DataFrame
  * 需求：用UDAF统计员工的平均薪资
  */
class UDAFDemo1 extends UserDefinedAggregateFunction {
  // 指定输入类型
  override def inputSchema: StructType = StructType(Array(StructField("salary", DoubleType, true)))
  // 缓冲的作用是将上次的结果和这次传进来的结果进行聚合，指定缓冲（分区）类型和聚合过程
  override def bufferSchema: StructType =
    StructType(StructField("sum", DoubleType) :: StructField("count", DoubleType) :: Nil)

  // 返回类型
  override def dataType: DataType = DoubleType

  // 如果给true，有相同的输入，该函数就有相同的输出
  // 如果输入的数据有不同的情况，比如每次数据有不同的时间或有不同的数据对应的offset，
  // 这时候得到的结果可能就不一样，这个值就设置为false
  override def deterministic: Boolean = true

  // 初始化方法，对buffer中的数据进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 员工的总薪资
    buffer(0) = 0.0
    // 员工的人数
    buffer(1) = 0.0
  }

  // 局部聚合，分区内的聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // buffer是指之前的结果，input是这次传进来的数据，将要和buffer进行聚合
    if (!input.isNullAt(0)) {
      // 聚合薪资
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      // 聚合人数
      buffer(1) = buffer.getDouble(1) + 1
    }
  }

  // 全局聚合，分区和分区的聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 合并薪资
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    // 合并人数
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }

  // 最终的结果，可以在该方法中对结果进行再次处理
  override def evaluate(buffer: Row): Any = buffer.getDouble(0) / buffer.getDouble(1)
}

object UDAFDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDFDemo1")
      .master("local")
      .getOrCreate()
    spark.udf.register("aggrfunc", new UDAFDemo1)

    val df = spark.read.json("D://data/employees.json")
    df.createOrReplaceTempView("employees")

    df.show()

    val res = spark.sql("select aggrfunc(salary) as avgsalary from employees")
    res.show()

    spark.stop()
  }
}
