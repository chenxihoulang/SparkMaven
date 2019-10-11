package com.chw.scala.spark

import org.apache.spark._
import org.apache.spark.streaming._

/**
 * mac上使用netcat模拟服务端
 * brew install netcat
 * netcat -l -p 9999
 */
object SocketWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SocketWordFreq")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}