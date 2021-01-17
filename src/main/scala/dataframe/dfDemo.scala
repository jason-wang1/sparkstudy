package com.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import ulits.SparkConfig

import scala.collection.mutable.ArrayBuffer

/**
  * Descreption: XXXX<br/>
  * Date: 2020年04月11日
  *
  * @author WangBo
  * @version 1.0
  */
case class GroupInsUptDel(insDF: DataFrame, uptDF: DataFrame, delDF: DataFrame)
case class GroupDeltUpsr(deltDF: DataFrame, upsrDF: DataFrame)

case class Person(id: Int,
                  name: String,
                  age: Int
                 )

case class People(bianhao: Int,
                  xingming: String,
                  nianling: String
                 )
case class MemberOrderInfo(area:String,memberType:String,product:String,price:Int)
case class StudentKey(name: String, score: Int)
case class DeviceData (id: Int, device: String)

import org.apache.spark.sql.functions._
object dfDemo {

  private def getNames(row: Row): List[String] = {
    val listofColumnNames = row.schema.fields.map(_.name).toList
    val listOfColumnWhichContainOne =  ArrayBuffer[String]()
    listofColumnNames.indices.foreach(index => {
      if(row.getInt(index).equals(1)) {
        listOfColumnWhichContainOne.append(listofColumnNames(index))
      }
    })
    listofColumnNames.toList
  }

  def my_function(columnName: String) : org.apache.spark.sql.Column = {
    val spark = new SparkConfig("dfDemo").getSparkSession
    import spark.implicits._
    when(
      $"l.$columnName" === $"r.$columnName", null
    ).otherwise($"l.$columnName").as(columnName)
  }

  def main(args: Array[String]): Unit = {
    val spark =  new SparkConfig("dfDemo").getSparkSession
    import spark.implicits._

//    val df1 = Seq(
//      ("id1", "test_name1", "test_address1", "test_code1", "V1"),
//      ("id2", "test_name2", "test_address2", "test_code2", "V2")
//    ).toDF("col_a", "col_b", "col_c", "col_d", "col_e")
//
//    val df2 = Seq(
//      ("id1", "test_name1.1", "test_address1.1", "test_code1.1", "V1"),
//      ("id4", "test_name4", "test_address4", "test_code4", "V4")
//    ).toDF("col_a", "col_b", "col_c", "col_d", "col_e")
//
//    val my_list = List("col_a", "col_b")
//    val my_list2 = List("col_c", "col_d", "col_e")
//    val joinDF = df1.as("l")
//      .join(df2.as("r"), df1("col_a") === df2("col_a"), "leftouter")
//      .select(col("l.col_c") :: col("l.col_d") :: col("l.col_e") :: my_list.map(my_function): _*)
//      .show()
//
//    val file_df = spark.read.format("csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load("D:\\data\\concrete.csv")
//      .show()


    val sc = spark.sparkContext
    //    val df: DataFrame = Seq(
//      ("2020", "English", 2),
//      ("2020", "English", 18),
//      ("2020", "Math", 3),
//      ("2019", "Math", 4),
//      ("2019", "English", 1),
//      ("2019", "Math", 2)
//    ).toDF("year", "course", "Age")

//    val df: DataFrame = Seq(
//      (2020, 2, 2),
//      (2019, 1, 2)
//    ).toDF("year", "course", "Age")

//    df.foreach(row => {
//      val strings: List[String] = getNames(row)
//      strings.foreach(println)
//      println("\n")
//    })


//    val df: DataFrame = Seq[(String, String, Double)](
//      ("2018-01-01 03:25:36", "项目1", 100),
//      ("2018-01-01 05:21:56", "项目2", 100),
//      ("2018-01-01 05:38:13", "项目3", 300),
//      ("2018-01-01 06:12:27", "项目1", 1000.7),
//      ("2018-01-01 06:25:36", "项目2", 200.2),
//      ("2018-01-01 06:39:31", "项目3", 999)
//    ).toDF("date", "project", "earning")

//    df.groupBy($"project")
//      .agg(collect_list($"earning").alias("earning_arr"))
//      .select($"*", posexplode($"earning_arr"))
//
//    df.select(to_json(struct($"*"))).show(false)


//    val jsonSchema: StructType = new StructType().add("device_id", IntegerType).add("device_type", StringType).add("ip", StringType).add("cn", StringType)
//    val devicesDF: DataFrame = json.select($"id", from_json($"json", jsonSchema).alias("device"))
//    devicesDF.printSchema()
//    devicesDF.show(false)
//
//    devicesDF.select($"id", $"device.*").show()


//      json.show(false)

//    val df: DataFrame = Seq[(String, String, Double)](("  2018-01 ", "项目1", 100), ("2018-01", "项目2", 100), ("2018-01", "项目3", 300),
//      ("2018-02", "项目1", 1000.7), ("2018-02", "项目2", 200.2), ("2018-03", "NaNss", 999)
//    ).toDF("date", "project", "earning")
//
//    val w: WindowSpec = Window.partitionBy("date").orderBy($"project".substr(3, 1))
//
//    df.select(
//      $"date", $"project", $"earning",
//      sum("earning").over(w.rangeBetween(Window.unboundedPreceding, 0)).alias("totalPrice"),
//      sum("earning").over(w.rowsBetween(Window.unboundedPreceding, 0)).alias("avgPrice")
//    ).show()

//    val frame: DataFrame = Seq[(Double, Double)]((180.54, 3), (5687.3, 2)).toDF("num1", "num2")


//    val memberDF: DataFrame = Seq(
//      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
//      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
//      MemberOrderInfo("深圳", "钻石会员", "钻石会员3个月", 70),
//      MemberOrderInfo("深圳", "钻石会员", "钻石会员12个月", 300),
//      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
//      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
//      MemberOrderInfo("深圳", "铂金会员", "铂金会员6个月", 120),
//      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
//      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
//      MemberOrderInfo("深圳", "黄金会员", "黄金会员3个月", 45),
//      MemberOrderInfo("深圳", "黄金会员", "黄金会员12个月", 180),
//      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
//      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
//      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
//      MemberOrderInfo("北京", "铂金会员", "铂金会员3个月", 60),
//      MemberOrderInfo("北京", "黄金会员", "黄金会员3个月", 45),
//      MemberOrderInfo("北京", "黄金会员", "黄金会员3个月", 45),
//      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
//      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
//      MemberOrderInfo("上海", "铂金会员", "铂金会员3个月", 60),
//      MemberOrderInfo("上海", "黄金会员", "黄金会员3个月", 45)
//    ).toDF("area", "memberType", "product", "price")
//      .selectExpr("area as yourarea", "memberType", "product", "price").alias("a").cache()

//    val newDF = Seq[(Integer,String,Integer)](
//      (2, "wang", null),
//      (3, "li", 61),
//      (4, "luo", 28),
//      (5, "zhou", 58)
//    ).toDF("Id", "Name", "Age")
//
//    newDF.select(
//      when($"id" > 3, 1)				// spark.sql.functions 中的 when
//        .when($"id" <= 3, 0)		// Column 中的 when
//        .otherwise(2).alias("flag"),
//      $"id"
//    ).show()
//    val tuple: Seq[(Integer, String, Integer)] =
//    Seq[(Integer,String,Integer)](
//      (2, "wang", null),
//      (3, "li", 61),
//      (4, "luo", 28))
//      tuple.tail

    val df: DataFrame = Seq(
      ("hi i heard about spark"),
      ("i wish java could use case classes")
    ).toDF("sentence")

    val splitWords: UserDefinedFunction = udf{sentence: String => sentence.split(" ")}
    val countTokens: UserDefinedFunction = udf{words: Seq[String] => words.length}

    df.withColumn("words", splitWords($"sentence"))
        .withColumn("count", countTokens($"words"))
        .show(false)

    sc.stop()
  }
}
