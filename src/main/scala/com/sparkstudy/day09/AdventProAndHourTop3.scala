package com.sparkstudy.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Descreption:
  * 需求二：统计每个省份每个小时的广告ID的top3
  * 1562085629599	Hebei	Shijiazhuang	 564	  1
  * 时间戳         省份  城市          用户id  广告id
  * Date: 2019年07月07日
  *
  * @author WangBo
  * @version 1.0
  */
object AdventProAndHourTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("AdventProAndHour").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    //导入数据
    val logs: RDD[String] = sc.textFile("D://data/Advert.log")

    //数据提取没有问题，但是整合得不好，导致后面又很多数据冗余。后续优化方案：
    //1. 把hour, pro, adid作为整体，而不是放在嵌套的元组里面
    //2. 把hour, pro, adid作为3元素元组
    val logsOfProHourAdid: RDD[(String, (String, String))] = logs.map(x => {
      val fields: Array[String] = x.split("\t")
      val hour: String = getHour(fields(0))
      val pro: String = fields(1)
      val adIi: String = fields(4)
      (pro, (hour, adIi))
    })
    logsOfProHourAdid

    //按省份聚合；RDD[(省份， Iterable[省份， (小时，广告id)])]
    val proGrouped: RDD[(String, Iterable[(String, (String, String))])] = logsOfProHourAdid.groupBy(_._1)

    //按小时聚合；RDD[(省份， Map[小时，Iterable[省份， (小时，广告id)]])]
    val hourProGrouped: RDD[(String, Map[String, Iterable[(String, (String, String))]])] = proGrouped.mapValues(_.groupBy(_._2._1))
//    for(i <- hourProGrouped.collect().toList) println(i)

    //按广告id聚合；RDD[(省份， Map[小时，Map[广告id，Iterable[省份， (小时，广告id)]]])]
    val adidHourProGrouped: RDD[(String, Map[String, Map[String, Iterable[(String, (String, String))]]])] = hourProGrouped.mapValues(_.mapValues(_.groupBy(_._2._2)))

    //以上三次聚合是为了计算相同的省份、小时、广告id访问记录出现的次数，既然这样，还不如一开始就把它们作为整体

    //求和；RDD[(省份， Map[小时，Map[广告id，数量]])]
    val sumed: RDD[(String, Map[String, Map[String, Int]])] = adidHourProGrouped.mapValues(_.mapValues(_.mapValues(_.size)))

    //排序
    val sorted: RDD[(String, Map[String, List[(String, Int)]])] = sumed.mapValues(_.mapValues(_.toList.sortWith(_._2 > _._2).take(3)))

    for (i <- sorted) println(i)

    sc.stop()

  }

  /**
    *
    * @param 时间
    * @return 小时
    */
  def getHour(time_string: String) : String = {
    val time: DateTime = new DateTime(time_string.toLong)
    time.getHourOfDay.toString
  }
}
