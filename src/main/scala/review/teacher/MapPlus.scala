package com.review.teacher

object MapPlus {
  def main(args: Array[String]): Unit = {
    val map1 = Map("user1" -> 10, "user2" -> 5)
    val map2 = Map("user2" -> 5,  "user3" -> 20)
    // Map("user1" -> 10, "user2" -> 10,  "user3" -> 20)

    println(map1 ++ map2)

    val map3 = map1.map { case (key, value) =>
      key -> (map2.getOrElse(key, 0) + value)
    }
    println(map3)
    println(map3 ++ map2)
  }
}
