package com.chw.scala.spark

object ParamT_test3 {
  def main(args: Array[String]): Unit = {
    val c1 = new Cell[String]("abc")

    //正常T,报错
    //    val c2: Cell[Any] = c1
    //    c2.set(1)


    val s: String = c1.get

    val a1 = Array("abc")
    //报错
    //    val a2:Array[Any] = a1

    val a2: Array[Any] = a1.asInstanceOf[Array[Any]]
    //错误
    //    a2(0) = 100

  }
}

class Cell[T](init: T) {
  private[this] var current = init

  def get = current

  /**
   * +T的时候,方法报错
   */
  def set(x: T) {
    current = x
  }
}
