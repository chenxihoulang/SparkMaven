package com.chw.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark版本的词频统计
 */
object SparkWC {
  def main(args: Array[String]): Unit = {
    println("spark word count ...")

    val conf = new SparkConf()
      .setAppName("word_count")
      .setMaster("local[2]")
//      .setMaster("spark://127.0.0.1:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val txtFile = "input.txt"
    val txtData: RDD[String] = sc.textFile(txtFile)

//    val txtData=sc.parallelize(Array("one","two","three","one"))

    txtData.cache()

    val lineCount = txtData.count()
    println(s"lineCount=$lineCount")

    val wcData = txtData.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wcData.collect().foreach(println)

    sc.stop
  }
}
