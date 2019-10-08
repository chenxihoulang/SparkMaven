package com.chw.scala.spark

import scala.reflect.ClassTag

object manifest_test {
  def foo[T](x: List[T])(implicit m: Manifest[T]) = {
    if (m <:< manifest[String])
      println("Hey, this list is full of strings")
    else
      println("Non-stringy list")
  }

  def foo1[T: Manifest](x: List[T]): Unit = {

  }

  def arrayMake[T: ClassTag](first: T, second: T) = {
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }

  def mkArray[T: ClassTag](elems: T*) = Array[T](elems: _*)

  def main(args: Array[String]): Unit = {
    foo(List("a", "b"))

    foo(List(1, 2, 3))


    val m: Manifest[A[String]] = manifest[A[String]]
    val function: Class[_] => ClassTag[A[String]] = ClassTag[A[String]]


    val ints: Array[Int] = arrayMake(1, 2)

    val strings: Array[String] = mkArray("a", "b")
  }
}

class A[T]

