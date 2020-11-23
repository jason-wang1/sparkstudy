package com.qf.day14

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}

/**
  * udaf操作DataSet
  */
// 生成DataSet的类型
case class Employee(name: String, salary: Double)
// 作为缓冲的类型
case class AvgSalary(var sum: Double, var count: Double)

class UDAFDemo2 extends Aggregator[Employee, AvgSalary, Double]{
  // 初始化方法，初始化每个buffer
  override def zero: AvgSalary = AvgSalary(0.0, 0.0)
  // 局部聚合
  override def reduce(buffer: AvgSalary, employee: Employee): AvgSalary = {
    buffer.sum += employee.salary // 聚合薪资
    buffer.count += 1 // 聚合人数
    buffer
  }
  // 全局聚合
  override def merge(b1: AvgSalary, b2: AvgSalary): AvgSalary = {
    b1.sum += b2.sum // 分区和分区的聚合，聚合薪资
    b1.count += b2.count // 聚合人数
    b1
  }
  // 计算结果
  override def finish(reduction: AvgSalary): Double = reduction.sum / reduction.count

  // 设置中间值的编码, 用的编码和Tuple和case是一样的
  override def bufferEncoder: Encoder[AvgSalary] = Encoders.product
  // 设置最终结果的编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object UDAFDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDFDemo2")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[Employee] = spark.read.json("dir/employees.json").as[Employee]

    ds.show

    // 指定某个列并调用udaf
    val avgsalary: TypedColumn[Employee, Double] = new UDAFDemo2().toColumn.name("avg_salary")
    val res: Dataset[Double] = ds.select(avgsalary)
    res.show

    spark.stop()
  }
}
