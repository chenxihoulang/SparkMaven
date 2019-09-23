package com.chw.scala.spark

object implicit_test3 {
  def main(args: Array[String]): Unit = {
    val bobsPrompt = new PreferredPrompt("登录成功")

    Greeter.greet("chw")(bobsPrompt)

    //    implicit val bobsPrompt1 = new PreferredPrompt("注册成功")

    import JamesPrefs._
    Greeter.greet("chw")

  }
}

class PreferredPrompt(val preference: String)

object Greeter {
  def greet(name: String)(implicit prompt: PreferredPrompt) {
    println("Welcome, " + name + ". The System is ready.")
    println(prompt.preference)
  }
}
