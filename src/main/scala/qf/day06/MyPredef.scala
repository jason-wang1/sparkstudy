package com.qf.day06

object MyPredef {
  //隐式方法，用于MyGirl类的对象的比较
  implicit def girlSelect(girl: MyGirl) = new Ordered[MyGirl]{
    override def compare(that: MyGirl) = {
      if (girl.faceValue != that.faceValue) {
        girl.faceValue - that.faceValue
      } else {
        that.age - girl.age
      }
    }
  }

  implicit object OrderingGirl extends Ordering[MyGirl]{
    override def compare(x: MyGirl, y: MyGirl): Int = {
      if (x.faceValue == y.faceValue) {
        y.age - x.age
      } else {
        x.faceValue - y.faceValue
      }
    }
  }

}
