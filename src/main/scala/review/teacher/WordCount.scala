package com.review.teacher

import scala.collection.mutable

object WordCount {
  def main(args: Array[String]): Unit = {
    val arr = Array("hadoop spark java", "scala spark hadoop hbase kafka hive sqoop")
    val words: Array[String] = arr.flatMap(_.split(" "))
    words.groupBy(x=>x).mapValues(_.length).foreach(println)

    val stringToInt: List[(String, Int)] = arr.flatMap(_.split(" "))
      .groupBy(x => x)
      .mapValues(_.length).toList.sortBy(_._2)

    stringToInt.foreach(println)

//    val result = mutable.Map[String, Int]()
//    for (word <- words){
//      if (result.contains(word))
//        result(word) = result(word) + 1
//      else
//        result(word) = 1
//    }
//    println(result)
//
//    val arr1 = (1 to 10).toArray
//    val resultx = arr1.reduce((buffer, elem) => {
//      println(s"buffer=$buffer; elem=$elem")
//      buffer+ elem
//    })
//
//    words.foldLeft(mutable.Map[String, Int]())((buffer, word) => {
//      result(word) = result.getOrElse(word, 0) + 1
//      result
//    })
//    words.foldRight()
  }
}
