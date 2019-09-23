package com.chw.scala.spark

import scala.math.{Pi => _, _}

import scala.Predef

object implicit_test1 {

  implicit def int2String(a: Int) = a.toString

  implicit def int2Tuple(a: Int) = Tuple1(a)

  implicit def double2Int(d: Double) = d.toInt

  implicit def int2double(x: Int): Double = x.toDouble

  def printEx(ex: RichException): Unit = {
    println(ex)
  }

  def main(args: Array[String]): Unit = {
    implicit_test1().printStr(1)

    val a: Int = 3.5

    printEx(new Throwable)
  }

  def apply(): implicit_test1 = new implicit_test1()
}

class implicit_test1 {
  def printStr(str: String) = println(str)
}

