package com.chw.scala.spark

import java.io.FileInputStream
import java.util.Properties

object ConfigUtils extends App {

  val props = new Properties

  //  Try(ConfigUtils.getClass.getClassLoader.getResourceAsStream("config.properties")) match {
  //    case Success(inputStream) =>{
  //      println(inputStream) //null
  //      println ("伪成功")
  //      props.load(inputStream)
  //    }
  //    case Failure(ex) => println(ex)
  //  }

  val properties = new Properties()

  try {
    val path = getClass.getResource("config.properties").getPath
    println(path)
  } catch {
    case e: Exception => println(e)
  }


  val path = "src/main/resources/config.properties"
  properties.load(new FileInputStream(path))
  println(properties.getProperty("url"))
}
