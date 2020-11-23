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
object joinTest {
  def main(args: Array[String]): Unit = {
    //初始化环境
    //Spark的配置类，可以灵活地配置用于运行应用程序的一些必要配置,
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkWC")  //指定应用程序名称，可以不指定，会生成一个默认的很长的字符串
    conf.setMaster("local[3]") //指定本地模式运行（local模式）。其中local是指调用一个线程运行任务；local[2]调用两个线程；local[*]会调用所有的空闲线程
    val sc: SparkContext = new SparkContext(conf)  //用于提交任务的入口类，也叫上下文对象

    //获取数据
    val rdd1 = sc.parallelize(Array((2,"b"),(2,"c"),(3,"d"),(4,"e")))
    val rdd2 = sc.parallelize(Array((1,4),(2,5),(2,6),(3,7)))

    val cogrouped: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cogrouped.collect().toList.foreach(println)
    
    //释放对象
    sc.stop()
  }

}
