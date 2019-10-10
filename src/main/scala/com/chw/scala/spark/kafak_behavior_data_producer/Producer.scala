package com.chw.scala.spark.kafak_behavior_data_producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object Producer extends App {
  //总数
  val events = 1000
  val topic = "userBehavior"
  val brokers = "localhost:9091,localhost:9092"

  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "userBehaviorGenerator")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  for (nEvents <- Range(0, events)) {
    // 生成模拟评论数据(timestamp, user, item)
    val timestamp = System.currentTimeMillis
    val user = rnd.nextInt(100)
    val item = rnd.nextInt(100)
    val data = new ProducerRecord[String, String](topic, user.toString, s"${timestamp}\t${user}\t${item}")

    //async
    //producer.send(data, (m,e) => {})
    //sync
    producer.send(data)
    if (rnd.nextInt(100) < 50) Thread.sleep(rnd.nextInt(10))
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}