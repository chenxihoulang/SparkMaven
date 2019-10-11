package com.chw.scala.spark.redis

import com.chw.scala.spark.performance_tuning.BroadcastWrapper
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.util.SizeEstimator

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

    val lines = ssc.socketTextStream("hadoop06", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.cache()

    val broadcast=BroadcastWrapper[Int](ssc,1)

    wordCounts.foreachRDD((rdd,time)=>{
      //定期根据广播变量
      if (time.greater(Time(10000))){
        broadcast.update(2,true)
      }

      //评估某些大内存变量的内存消耗
      val memorySize: Long = SizeEstimator.estimate(broadcast)
      println(s"memorySize=$memorySize")

      rdd.foreachPartition(it=>{
        RedisUtils.checkAlive
        while (it.hasNext){
          //保存数据到redis中
        }
      })
    })

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}