package com.chw.scala.spark.alarm.util

/**
 * @author litaoxiao
 * configuration
 */
object Conf extends Serializable {
  // parameters configuration
  val nGram = 4
  val updateFreq = 3600000 //60min

  // api configuration
  val segmentorHost = "http://localhost:8282"

  // spark configuration
//  val master = "spark://localhost:7077"
  val master = "local[2]"
  val localDir = "./tmp"
  val perMaxRate = "5"
  val interval = 3 // seconds
  val parallelNum = "15"
  val executorMem = "1g"
  val concurrentJobs = "5"
  val coresMax = "3"

  // kafka configuration
  val brokers = "localhost:9091,localhost:9092"
  val zk = "localhost:2181"
  val group = "MASGroup"
  val topics = List("monitorAlarm")
  val outTopics = "monitorAlarmOut"

  // mysql configuration
  val mysqlConfig = Map("url" -> "jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8", "username" -> "root", "password" -> "123456")
  val maxPoolSize = 5
  val minPoolSize = 2
}