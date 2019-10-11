package com.chw.scala.spark

object trait_test2 {
  def main(args: Array[String]): Unit = {
    println(Demo)
    println(Demo.x)

    val x = 2
    var lazyRationalTrait = new LazyRationalTrait {
      val numerArg = x
      val denomArg = 2 * x

      println("LazyRationalTrait子类初始化")
    }

    println(lazyRationalTrait)
  }
}

object Demo {
  lazy val x = {
    println("initializing x");
    "done"
  }
}

trait LazyRationalTrait {
  val numerArg: Int
  val denomArg: Int

  println("LazyRationalTrait初始化")

  lazy val numer = numerArg / g
  lazy val denom = denomArg / g

  private lazy val g = {
    require(denomArg != 0)
    gcd(numerArg, denomArg)
  }

  private def gcd(a: Int, b: Int): Int =
    if (b == 0) a else gcd(b, a % b)

  override def toString = numer + "/" + denom
}
