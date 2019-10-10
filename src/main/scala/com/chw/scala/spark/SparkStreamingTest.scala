package com.chw.scala.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTest extends App {
  val conf = new SparkConf()
    .setAppName("SocketWordFreq")
    .setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))
  ssc.sparkContext.setLogLevel("error")
  ssc.checkpoint("ck_updateStateByKey")
  val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop06", 9999)

  val words: DStream[(String, Int)] = lines.map((_, 1))

  private val value: DStream[(String, Int)] = words.reduceByKeyAndWindow((v1: Int, v2: Int) => (v1 + v2), Seconds(20), Seconds(10))
  //  value.print

  private val value1: DStream[(Int, String)] = value.transform(one => {
    println("transform")
    one.map(t => (t._2, t._1))
  })

  //  value1.foreachRDD(rdd => {
  //    println("diver端执行")
  //
  //    rdd.foreach(one => {
  //      println(s"executor端执行:${one._1},${one._2}")
  //    })
  //  })

  value1.foreachRDD((rdd, time) => {
    println(s"diver端执行:$time")

    rdd.foreach(one => {
      println(s"executor端执行:${one._1},${one._2}")
    })
  })

  //  val wordCount: DStream[(String, Int)] = words.updateStateByKey((newValues: Seq[Int], runningCount: Option[Int]) => {
  //    var sum = 0
  //    for (v <- newValues) {
  //      sum += v
  //    }
  //
  //    val old = runningCount.getOrElse(0)
  //
  //    Some(sum + old)
  //  })
  //
  //  wordCount.print()

  ssc.start()
  ssc.awaitTermination()
}
