package com.quickscala

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
  * Descreption: XXXX<br/>
  * Date: 2020年04月19日
  *
  * @author WangBo
  * @version 1.0
  */
object charpter05 {
  /*
   * 5.1
   * 改进5.1节的Counter类, 让它不要在Int.MaxValue时变成负数
   */
  def question1: Unit = {
    class Counter {
      @BeanProperty var value = 0
      def increment() {
        if (value < Int.MaxValue)
          value += 1
      }
      def current() = value
    }
    val counter = new Counter
    counter.setValue(Int.MaxValue)
    counter.increment()
    println(counter.getValue)
  }


  /*
   * 5.2
   * 编写一个BankAccount类, 加入deposit和withdraw方法,和一个只读 的balance属性
   */
  def question2: Unit = {
    class BankAccount {
      private[this] var _balance: Double = 0

      def deposit(n: Double) = {
        _balance += n
      }
      def withdraw(n: Double) = {
        _balance -= n
      }
      def getBalance() = _balance
    }

    val bankAccount = new BankAccount
    bankAccount.deposit(100)
    println(bankAccount.getBalance())

  }


  /*
   * 5.3
   * 编写一个Time类, 假如只读属性hours和minutes,
   * 和一个检查某一时刻是否遭遇另一时刻的方法before(other: Time): Boolean,
   * Time对象应该以new Time(hrs, min)方式构建,
   * 其中hrs小时数以军用时间格式呈现
   */
  def question3(): Unit = {
    class Time(val hours: Int, val minites: Int) {
      def before(other: Time): Boolean = {
        if (hours < other.hours) true
        else if (hours > other.hours) false
        else minites < other.minites
      }
      println(hours)
    }
    val time1 = new Time(3, 18)
    val time2 = new Time(4, 12)
    println(time1.before(time2))
  }

  /*
   * 5.4
   * 重新实现前一个练习中的Time类, 将内部呈现改成自午夜起的分钟数(介于0到24*60-1之间),
   * 不要改变公有接口, 也就是说, 客户端代码不应因你的修改而收到影响
   */
  def question4(hr: Int, min: Int): Unit = {
    class Time(val hours: Int, val minites: Int) {
      val mm = hours * 60 + minites
      def before(other: Time): Boolean = {
        if (mm < other.mm) true
        else false
      }
      println(hours)
    }
    val time1 = new Time(3, 18)
    val time2 = new Time(4, 12)
    println(time1.before(time2))
  }

  /*
   * 5.5
   * 创建一个Student类, 加入可读写的JavaBeans属性name(类型为String)和id(类型为Long),有哪些方法被生成?(用javap查看)
   * 你可以在Scala中调用JavaBeans版的getter和setter方法吗?应该这样做吗?
   */
  def question5: Unit = {
    class Student(@BeanProperty var name: String, @BeanProperty var id: Long) {

      override def toString = s"Student($name, $id)"
    }
    val zhangsan = new Student("zhangsan", 18)
    zhangsan.id = 19
    println(zhangsan)
  }

  /*
   * 5.6
   * 在5.2的Person类中提供一个主构造器, 将负年龄转换为0
   */
  def question6: Unit = {
  }

  /*
   * 5.7
   * 编写一个Person类, 其主构造器接受一个字符串, 该字符串包含名字 空格和姓,
   * 如new Person("Fred Smith")
   * 提供只读属性firstName和lastName, 主构造器参数应该是var val 还是普通参数呢? 为什么?
   */
  def question7: Unit = {
    class Person(name: String) {
      val firstName: String = name.split(" ")(0)
      val lastName: String = name.split(" ")(1)
    }
    val person = new Person("Fred Smith")
    println(person.firstName)
    println(person.lastName)
  }

  /*
   * 5.8
   * 创建一个Car类, 以只读属性对应制造商  型号名称  型号年份以及一个可读写的属性用于车牌,
   * 提供四组构造器, 每一个构造器都要求制造商和型号名称为必填,
   * 型号年份和车牌为可选,如果未填, 则型号年份设置为-1, 车牌设置为空字符串,
   * 你会选择哪一个作为你的主构造器?为什么?
   */
  def question8: Unit = {
    class Car(val producer: String, val modelName: String, val modelYear: Int = -1) {
      var carCode: String = ""
      def this(manu: String, model: String, carCode: String) {
        this(manu, model)
        this.carCode = carCode
      }
      def this(producer: String, modelName: String, modelYear: Int, carCode: String) {
        this(producer, modelName, modelYear)
        this.carCode = carCode
      }

      override def toString = s"Car($producer, $modelName, $modelYear, $carCode)"
    }
    val car = new Car("fute", "aa", 2018, "bb")
    println(car)
  }

  /*
   * 5.9
   * 在Java  C#  C++ 中选择一个, 重写前一个练习, Scala相比之下精简多少?
   */
  def question9: Unit = {
    //ignore
  }

  /*
   * 5.10
   * 考虑如下类:
   * class Employee(val name:String, var salary: Double) {
   *   def this() { this("John Q. Public", 0.0) }
   * }
   * 重写该类, 使用显示的字段定义, 和一个缺省的主构造器,
   * 你更倾向于使用哪一种形式? 为什么?
   */
  def question10: Unit = {
    class Employee(val name:String = "John Q. Public", var salary: Double = 0.0) {

      override def toString = s"Employee($name, $salary)"
    }
    val employee = new Employee()
    println(employee)
  }

  def main(args: Array[String]): Unit = {

//    val chatter = new NetWork
//    val myFace = new NetWork
//
//    val fred: chatter.Member = chatter.join("Fred")
//    val wilma: chatter.Member = chatter.join("Wilma")
//
//    fred.contacts += wilma
//
//    val barney: myFace.Member = myFace.join("Barney")
//    fred.contacts += barney

    question10

  }
}

class NetWork {
  class Member(val name: String) {
    val contacts = new ArrayBuffer[Member]()
  }

  private val members = new ArrayBuffer[Member]

  def join(name: String): Member = {
    val m = new Member(name)
    members += m
    m
  }
}