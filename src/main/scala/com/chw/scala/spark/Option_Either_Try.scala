package com.chw.scala.spark

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Option:有值Some(T),无值None
 * Either:要么是Left,要么是Right
 * Try:可能抛出异常,成功是Success(T),抛异常是Failure(Throwable)
 */
object Option_Either_Try extends App {
  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }

  val bag = List("1", "2", "foo", "4", "bar")
  //筛选出整数

  private val flatten: List[Int] = bag.map(toInt).flatten
  flatten.foreach(println)

  private val ints: List[Int] = bag.flatMap(toInt)
  ints.foreach(println)

  private val ints1: List[Int] = bag.map(toInt).collect({ case Some(t) => t })
  ints1.foreach(println)


  def printContentLength(x: Option[String]): Unit = {
    x.map("length: " + _.length).foreach(println)
  }

  //打印字符串长度
  val value1 = Some("value1")
  val value2 = None

  printContentLength(value1) //length： 6
  printContentLength(value2) //无打印


  def trimUpper(x: Option[String]): Option[String] = {
    x.map(_.trim).filter(!_.isEmpty).map(_.toUpperCase)
  }

  //修剪字符串,并转化为大写
  val name1 = Some("  name  ")
  val name2 = None

  println(trimUpper(name1)) //Some(NAME)
  println(trimUpper(name2)) //None


  //可能抛出异常的代码,使用Try进行包装
  //得到Try的子类Success或者Failure，如果计算成功，返回Success的实例，如果抛出异常，返回Failure并携带相关信息。
  def divideBy(x: Int, y: Int): Try[Int] = {
    Try(x / y)
  }

  println(divideBy(1, 1).getOrElse(0)) // 1
  println(divideBy(1, 0).getOrElse(0)) //0
  divideBy(1, 1).foreach(println) // 1
  divideBy(1, 0).foreach(println) // no print

  divideBy(1, 0) match {
    case Success(i) => println(s"Success, value is: $i")
    case Failure(s) => println(s"Failed, message is: $s")
  }

  def readTextFile(filename: String): Try[List[String]] = {
    Try(Source.fromFile(filename).getLines.toList)
  }

  val filename = "/etc/passwd"
  readTextFile(filename) match {
    case Success(lines) => lines.foreach(println)
    case Failure(f) => println(f)
  }

  //返回值泛型参数,代表Left和Right的值的类型
  def divideBy2(x: Int, y: Int): Either[String, Int] = {
    if (y == 0) Left("Dude, can't divide by 0")
    else Right(x / y)
  }

  divideBy2(1, 0) match {
    case Left(s) => println("Answer: " + s)
    case Right(i) => println("Answer: " + i)
  }
}
