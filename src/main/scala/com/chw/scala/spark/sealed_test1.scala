package com.chw.scala.spark

object sealed_test1 {
  def main(args: Array[String]): Unit = {

  }

  def describe(e: Expr): String =
    e match {
      case Number(_) => "a number"
      case Var(_) => "a variable"
      case _ => "none"
    }

  def describe1(e: Expr): String =
    (e: @unchecked) match {
      case Number(_) => "a number"
      case Var(_) => "a variable"
    }
}