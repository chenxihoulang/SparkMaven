package com.chw.scala.spark

object Test1 extends App {
  val log = List(Array("1", "2"), Array("3", "4"))
  //将log进行map映射,转换为_对象的第0个元素,_(0)是一体的,并且转换为Long类型
  val longs: List[Long] = log.map(_ (0).toLong)

  longs.foreach(println)

  //上面类似于下面
  val longs1: List[Long] = log.map(arr => arr(0).toLong)

  longs1.foreach(println)
}
