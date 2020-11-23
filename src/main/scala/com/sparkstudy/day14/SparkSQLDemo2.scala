package com.sparkstudy.day14

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: RDD--->DataFrame 通过反射推断Schema
  */
object SparkSQLDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL2").setMaster("local")
    val sc = new SparkContext(conf)

    val data: RDD[Array[String]] = sc.textFile("D://data/people.txt").map(_.split(","))

    //用样例类的方式将数据进行类型映射
    val tupRDD: RDD[Person] = data.map(x => Person(x(0), x(1).trim.toInt))

    //创建SparkSQL初始化环境
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val frame: DataFrame = spark.read.json("")
    val ds: Dataset[Person] = tupRDD.toDS()
    val df: DataFrame = tupRDD.toDF()

    val ds2: Dataset[Person] = df.as[Person]
    val df2: DataFrame = ds.toDF()

    df.write.save("")


    ds.show()
    df.show()


    ds.map(line => {
      val name: String = line.name  //在编译时就知道字段的类型
      val age: Int = line.age
      (name, age)
    }).show()

    df.map(line => {
      val name: Nothing = line.getAs("name")  //在编译时不知道字段的类型
      val age: Nothing = line.getAs("age")
      (name, age)
    }).show()  //报错

    //对于DataFrame，可以用getAs获取字段
    val analysed1: Dataset[(String, Int)] = df.map(line => {
      val name: String = line.getAs("name")
      val age: Int = line.getAs("age")
      (name, age)
    })

    //对于DataSet，每一行是强类型，这里是Person，可以很轻松地用 对象.属性 获取每一行特定的字段
    val analysed2: Dataset[(String, Int)] = ds.map(line => {
      val name: String = line.name
      val age: Int = line.age
      (name, age)
    })
    analysed2


    sc.stop()
    spark.stop()
  }
}

//构建样例类，此时用于生成Schema信息
case class Person(name: String, age: Int)