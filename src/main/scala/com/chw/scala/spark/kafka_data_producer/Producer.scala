package com.chw.scala.spark.kafka_data_producer

import java.util.Properties

import com.chw.scala.spark.kafka.Producer.rnd
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource
import scala.util.Random

/**
 * 用于生成模拟数据的生产者
 */
object Producer extends App {
  //总的评论数
  val events = 1000
  val topic = "GameUserComment"
  val brokers = "localhost:9091,localhost:9092"
  val rnd = new Random()
  val props = new Properties()

  props.put("bootstrap.servers", brokers)
  props.put("client.id", "wordFreqGenerator")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  //构建Kafka生产者,泛型参数分别为Key的类型和值的类型
  val producer = new KafkaProducer[String, String](props)
  val startTime = System.currentTimeMillis()

  // 读取汉字字典
  val source: BufferedSource = scala.io.Source.fromFile("hanzi.txt")

  val lines = try {
    source.mkString
  } finally {
    source.close()
  }

  for (nEvents <- Range(0, events)) {
    // 生成模拟评论数据(user, comment)
    val sb = new StringBuilder()

    //随机从字典中抽取汉字拼接在一起
    for (ind <- Range(0, rnd.nextInt(200))) {
      sb += lines.charAt(rnd.nextInt(lines.length()))
    }

    val userName = "user_" + rnd.nextInt(100)
    //构造生产者记录
    val data = new ProducerRecord[String, String](topic, userName, sb.toString())

    //async
    //producer.send(data, (m,e) => {})
    //sync
    producer.send(data)
  }

  System.out.println("每秒发送事件: " + events * 1000 / (System.currentTimeMillis() - startTime))

  producer.close()
}