package com.chw.scala.spark

object caseclass_test1 {

  def main(args: Array[String]): Unit = {
    val x = Var("x")
    println(x.name)

    val op = BinOp("+", Number(1), x)

    println(op.right == Var("x"))

    val op1 = op.copy(operator = "-")

    println(simplifyTop(BinOp("*", x, Number(1))))

    List(0, 2, 4) match {
      case List(0, _, _) => println("found it ")
      case _ =>
    }

    (1, 2, 3) match {
      case (a, b, c) => println("matched " + a + ":" + b + ":" + c)
      case _ =>
    }

    println(generalSize("abc"))
    println(generalSize(Map(1 -> "1")))
    println(generalSize(null))


    println(isStringArray(Array(1, 2, 3)))
    println(isStringArray(Array("1", "2")))


    UnOp("abs", UnOp("abs", Var("x"))) match {
      case UnOp("abs", e@UnOp("abs", _)) => println(e)
      case _ =>
    }

    simplifyAdd(BinOp("+", Var("x"), Var("x")))
    simplifyAdd(BinOp("+", Var("x"), Var("y")))

  }

  def simplifyAdd(e: Expr) =
    e match {
      case BinOp("+", x, y) if x == y => println("x==y", x, y)
      case _ =>
    }


  def simplifyTop(expr: Expr): Expr =
    expr match {
      case UnOp("-", UnOp("-", e)) => e
      case BinOp("+", e, Number(0)) => e
      case BinOp("*", e, _) => e
      case _ => expr
    }

  def generalSize(x: Any) = x match {
    case s: String => s.length
    case m: Map[_, _] => {
      m.size
    }
    case _ => -1
  }

  def isStringArray(x: Any) =
    x match {
      case a: Array[String] => "string"
      case a: Array[Int] => "int"
      case _ => "no"
    }


  /**
   * simplifyAll 规则 1 是规则 4 的特例，而规则 2，3 是规则 5 的特例，因此 1，2，3 需要定义在 4，5 之前，
   * 而最后的缺省规则需要放在最后，如果顺序反了，比如把通用的规则放在特例之前，编译器会给出警告，
   * 因为当 Scala 做匹配时，按照规则定义的顺序，首先匹配通用规则，规则匹配成功，后面的特例没有机会匹配
   */
  def simplifyAll(expr: Expr): Expr =
    expr match {
      case UnOp("-", UnOp("-", e)) => e
      case BinOp("+", e, Number(0)) => e
      case BinOp("*", e, Number(1)) => e
      case UnOp(op, e) => UnOp(op, simplifyAll(e))
      case BinOp(op, l, r) => BinOp(op, simplifyAll(l), simplifyAll(r))
      case _ => expr
    }
}

sealed abstract class Expr

case class Var(name: String) extends Expr

case class Number(num: Double) extends Expr

case class UnOp(operator: String, arg: Expr) extends Expr

case class BinOp(operator: String, left: Expr, right: Expr) extends Expr

