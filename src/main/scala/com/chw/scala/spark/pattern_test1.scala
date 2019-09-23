package com.chw.scala.spark

object pattern_test1 {
  def main(args: Array[String]): Unit = {
    val tp2 = (1, "2")

    val (age, name) = tp2

    println(age, name)

    val exp = new BinOp("*", Number(5), Number(1))

    val BinOp(op, left, right) = exp

    println(op)
    println(left)
    println(right)


    val withDefault: Option[Int] => Int = {
      case Some(x) => x
      case None => 0
      case _ => -1
    }

    println(withDefault(Some(1)))
    println(withDefault(None))
    println(withDefault(null))


    val second: PartialFunction[List[Int], Int] = {
      case x :: y :: _ => y
    }

    if (second.isDefinedAt(List(1, 2, 3))) {
      println(second(List(1, 2, 3)))
    } else {
      println("match error")
    }

    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo", "China" -> "Beijing")

    for ((key, value) <- capitals) {
      println(key, value)
    }

    val results = List(Some("apple"), None, Some("Orange"))

    for (Some(fruit) <- results) println(fruit)
  }
}