package com.chw.scala.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 程序运行过程中,通过命令将某个文件cp到input目录中
 */
object Save2File {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Save2File_SparkStreaming")
      .setMaster("local[2]")
    // Create spark streaming context
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setLogLevel("error")

    val input = "/Users/chaihongwei/maven/demo/SparkMaven/input"
    val output = "/Users/chaihongwei/maven/demo/SparkMaven/output"
    println("read file name: " + input + "\nout file name: " + output)

    // 从磁盘上读取文本文件作为输入流,只会监控指定目录新建立的文件内容
    val textStream = ssc.textFileStream(input)
    // 进行词频统计
    val wcStream = textStream.flatMap { line => line.split(" ") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)
    // 打印到控制台并保存为文本文件和序列化文件
    wcStream.print()

    //将结果保存到文本文件中
    wcStream.saveAsTextFiles("file://" + output + "/saveAsTextFiles")
    //将结果保存到序列化文件中
    wcStream.saveAsObjectFiles("file://" + output + "/saveAsObjectFiles")

    ssc.start()
    ssc.awaitTermination()
  }
}