package com.chw.scala.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
 * 在当前batch没有对应key的更新时,
 * mapWithState:不会输出update信息
 * updateStateByKey:每次都输出所有key对应的update信息
 */
object MapWithState_UpdateStateByKey extends App {
  val conf = new SparkConf()
    .setAppName("SocketWordFreq")
    .setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))
  ssc.sparkContext.setLogLevel("error")
  ssc.checkpoint("ck_state")
  val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

  val words: DStream[(String, Int)] = lines.map((_, 1))

  words.cache()

  words.updateStateByKey((newValues: Seq[Int], oldValue: Option[Int]) => {
    println(s"newValues:${newValues.mkString(", ")}")

    Some(newValues.foldLeft(oldValue.getOrElse(0))(_ + _))
  }).foreachRDD(rdd => {
    rdd.foreach(println)
  })

  //一个映射函数,保存整数状态并返回一个字符串,其中state存储了batch数据之前的状态
  def mappingFunction(key: String, value: Option[Int], state: State[Int]) = {
    //使用 state.exists(), state.get(), state.update() and state.remove()来管理状态,并返回必须的字符串
    val sum = value.getOrElse(0) + state.getOption().getOrElse(0)

    state.update(sum)
    println(s"新值:$value")

    (key, sum)
  }

  val spec = StateSpec.function(mappingFunction _).numPartitions(10)

  words.mapWithState(spec).foreachRDD(rdd => {
    rdd.foreach(println)
  })


  ssc.start()
  ssc.awaitTermination()

}
