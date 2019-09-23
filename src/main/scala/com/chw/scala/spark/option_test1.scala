package com.chw.scala.spark

object option_test1 {
  def main(args: Array[String]): Unit = {
    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo", "China" -> "Beijing")

    var maybeString: Option[String] = capitals.get("China")
    maybeString = capitals.get("English")

    maybeString match {
      case Some(city) => println(city)
      case None => println("?")
    }
  }
}