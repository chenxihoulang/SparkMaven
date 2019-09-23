package com.chw.scala.spark

object trait_test1 {
  def main(args: Array[String]): Unit = {
    val rational = new RationalTrait {
      override val numerArg: Int = 100
      override val denomArg: Int = 200
    }

    println(rational.numerArg + "/" + rational.denomArg)

    val x = 1

    val rational1 = new {
      val numerArg = 1 * x
      val denomArg = 2 * x
    } with RationalTrait1

    println(rational1)

    object twoThirds extends {
      val numerArg = 1 * x
      val denomArg = 2 * x
    } with RationalTrait

    println(twoThirds.numerArg + "/" + twoThirds.denomArg)

    new RationalClass1(10, 20)
  }
}

trait RationalTrait {
  val numerArg: Int
  val denomArg: Int
}

trait RationalTrait1 {
  val numerArg: Int
  val denomArg: Int

  require(denomArg != 0)

  private val g = gcd(numerArg, denomArg)

  val numer = numerArg / g
  val denom = denomArg / g

  private def gcd(a: Int, b: Int): Int =
    if (b == 0) a else gcd(b, a % b)

  override def toString = numer + "/" + denom
}


class RationalClass1(n: Int, d: Int) extends {
  val numerArg = n
  val denomArg = d
} with RationalTrait1 {
  println(numerArg + "/" + denomArg)
}
