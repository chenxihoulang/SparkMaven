package com.chw.scala.spark


object implicit_test2 {


  implicit def rational2Int(r: Rational) = r.denom

  implicit def int2Rational(x: Int) = new Rational(x)

  def main(args: Array[String]): Unit = {
    val oneHalf = new Rational(1, 2)

    println(oneHalf + oneHalf)

    println(oneHalf + 1)

    println(1 + oneHalf)

    val map = Map("1" -> 1, "2" -> 2)

    val mapByTuple2 = Map(("1", 1), ("2", 2))
  }
}

class Rational(n: Int, d: Int) {
  require(d != 0)
  private val g = gcd(n.abs, d.abs)
  val numer = n / g
  val denom = d / g

  def this(n: Int) = this(n, 1)

  override def toString = numer + "/" + denom

  def +(that: Rational) =
    new Rational(
      numer * that.denom + that.numer * denom,
      denom * that.denom
    )

  def +(i: Int): Rational =
    new Rational(numer + 1 * denom, denom)

  def *(that: Rational) =
    new Rational(numer * that.numer, denom * that.denom)

  private def gcd(a: Int, b: Int): Int =
    if (b == 0) a else gcd(b, a % b)

}

object JamesPrefs {
  implicit val prompt = new PreferredPrompt("Yes, master> ")
}