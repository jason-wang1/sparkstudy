package com.sparkstudy.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月02日
  *
  * @author WangBo
  * @version 1.0
  */
object KeyValueRDDTest {
  def main(args: Array[String]): Unit = {
    //初始化环境
    //Spark的配置类，可以灵活地配置用于运行应用程序的一些必要配置,
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkWC")  //指定应用程序名称，可以不指定，会生成一个默认的很长的字符串
    conf.setMaster("local[3]") //指定本地模式运行（local模式）。其中local是指调用一个线程运行任务；local[2]调用两个线程；local[*]会调用所有的空闲线程
    val sc: SparkContext = new SparkContext(conf)  //用于提交任务的入口类，也叫上下文对象

    //获取数据
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)

    val combined: RDD[(String, (Int, Int))] = rdd.combineByKey(v => (v, 1),
      (C: (Int, Int), v: Int) => (C._1 + v, C._2 + 1),
      (C1: (Int, Int), C2: (Int, Int)) => (C1._1 + C2._1, C1._2 + C2._2))

    val avgByKey: RDD[(String, Double)] =
      combined.mapValues{case(sum, num) => sum/num.toDouble}

    avgByKey.collect().toList.foreach(println)
    
    //释放对象
    sc.stop()
  }

}
