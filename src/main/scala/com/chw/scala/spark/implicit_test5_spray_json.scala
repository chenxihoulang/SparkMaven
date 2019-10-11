package com.chw.scala.spark

import spray.json.DefaultJsonProtocol._
import spray.json._

object implicit_test5 extends App {
  implicit def pimpAny[T](any: T) = new ImplicitTest1(any)

  //1.优先在当前上下文中查找隐式对象
  //implicit val test: ImplicitTest2 = new ImplicitTest2("隐式上下文中创建")

  //0.将1隐式转换为ImplicitTest1,使用上面的隐式转换函数
  1.println

  val str: String = Map("gameId" -> 1, "review" -> 2).toJson.toString()
  println(str)

  val jsonAst = List(1, 2, 3).toJson
  println(jsonAst)


  case class Color(name: String, red: Int, green: Int, blue: Int)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat4(Color)
    implicit val attrFormat = jsonFormat2(Attribute)
    implicit val personFormat = jsonFormat2(Person)
  }

  import MyJsonProtocol._
  //导包的时候一定要注意别导重复了,否则会报错

  val json = Color("CadetBlue", 95, 158, 160).toJson
  val color = json.convertTo[Color]


  //解析嵌套类型
  case class Person(username: String, attribute: Attribute)

  case class Attribute(age: Int, weight: Int)


  val jsonStr = """{"username":"Ricky", "attribute": {"age":12, "weight":60}}"""
  val jsonObj = jsonStr.parseJson.convertTo[Person]

  val username = jsonObj.username
  val userAge = jsonObj.attribute.age

  println(s"$username $userAge")


  val persons = Array(Person("chw", Attribute(10, 10)), Person("chaihongwei", Attribute(100, 100)))
  val personListJson: String = persons.toJson.toString()
  println(personListJson)

  val listPersonJson = """[{"username":"chw","attribute":{"age":10,"weight":10}},{"username":"chaihongwei","attribute":{"age":100,"weight":100}}]"""

  val persons1: Array[Person] = listPersonJson.parseJson.convertTo[Array[Person]]
  persons1.foreach(println)
}

class ImplicitTest1[T](name: T) {
  //需要ImplicitTest2类型的隐式参数
  def println(implicit test1: ImplicitTest2): Unit = {
    test1.print1
  }

}

object ImplicitTest2 {
  //2.如果在对应的上下文中没有找到隐式对象,这在隐式对象的伴生类对象中查找
  implicit val test: ImplicitTest2 = new ImplicitTest2("伴生对象中创建")
}

class ImplicitTest2(name: String) {

  def print1 {
    println(this.toString + name)
  }
}
