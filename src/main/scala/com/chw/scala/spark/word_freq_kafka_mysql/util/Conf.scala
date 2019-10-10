package com.chw.scala.spark.word_freq_kafka_mysql.util

/**
 * 配置信息
 */
object Conf extends Serializable {
  // parameters configuration
  val nGram = 3
  val updateFreq = 300000 //5min

  //分词服务地址
  val segmentorHost = "http://localhost:8282"

  // spark configuration
  val master = "spark://localhost:7077"
  val localDir = "/Users/xiaolitao/Program/scala/data/tmp"
  val perMaxRate = "5"
  val interval = 3 // seconds
  val parallelNum = "15"
  val executorMem = "1G"
  val concurrentJobs = "5"
  val coresMax = "3"

  // kafka configuration
  val brokers = "localhost:9091,localhost:9092"
  val zk = "localhost:2181"
  val group = "wordFreqGroup"
  val topics = "test"

  // mysql configuration
  val mysqlConfig = Map("url" -> "jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8", "username" -> "root", "password" -> "123456")
  val maxPoolSize = 5
  val minPoolSize = 2
}
