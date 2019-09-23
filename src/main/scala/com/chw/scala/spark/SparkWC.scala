package com.chw.scala.spark

import org.apache.spark.SparkConf

/**
 * spark版本的词频统计
 */
object SparkWC {
  def main(args: Array[String]): Unit = {
    println("spark word count ...")
    val conf = new SparkConf()
  }
}

class SparkWC {
  def getName() = SparkWC.getClass.getSimpleName
}
