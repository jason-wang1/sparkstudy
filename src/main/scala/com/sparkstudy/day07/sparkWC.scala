package com.sparkstudy.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年07月02日
  *
  * @author WangBo
  * @version 1.0
  */
object sparkWC {
  def main(args: Array[String]): Unit = {
    //初始化环境
    //Spark的配置类，可以灵活地配置用于运行应用程序的一些必要配置,
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkWC")  //指定应用程序名称，可以不指定，会生成一个默认的很长的字符串
    conf.setMaster("local[2]") //指定本地模式运行（local模式）。其中local是指调用一个线程运行任务；local[2]调用两个线程；local[*]会调用所有的空闲线程
    val sc: SparkContext = new SparkContext(conf)  //用于提交任务的入口类，也叫上下文对象

    //获取数据
    val lines: RDD[String] = sc.textFile("hdfs://node01:9000/hello.txt")


    //分析数据
    val words: RDD[String] = lines.flatMap(_.split(" ")) //切分生成一个个单词
    val tups: RDD[(String, Int)] = words.mapPartitions(dataOfPartition => {
      dataOfPartition.map((_, 1))
    })

    val tuples: RDD[(String, Int)] = words.map((_, 1)) //生成一个个元组
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_+_) //聚合
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2, false)
    
    //结果存储展示
    println(sorted.collect().toBuffer)
    sorted.saveAsTextFile("hdfs://node01:9000/out")
    //释放对象
    sc.stop()
  }

}
