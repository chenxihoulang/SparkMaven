package com.chw.scala.spark

object UnderLine_Use extends App {

  def printArgs(ages: Int*) {
  }

  printArgs(Array(1, 2, 3): _*)

  def printList(list: List[_]): Unit = {
    list.foreach(println)
  }

  class Foo {
    def name = {
      "foo"
    }

    //name_=相当于方法名称
    def name_=(str: String) {
      println("set name " + str)
    }

  }

  val m = new Foo()
  m.name = "Foo"
  m.name_=("Foo")

  def sum(a: Int, b: Int, c: Int) = a + b + c
  val b = sum(1, _:Int, 3)
  println(b(2))

}

