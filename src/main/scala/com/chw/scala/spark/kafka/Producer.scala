package com.chw.scala.spark.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object Producer extends App {
  val topic = "kafkaOperation"
  val brokers = "localhost:9091,localhost:9092"
  val rnd = new Random()

  //参考:http://kafka.apachecn.org/documentation.html#producerconfigs
  val props = new Properties()
  //这是一个用于建立初始连接到kafka集群的"主机/端口对"配置列表。
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  //发出请求时传递给服务器的 ID 字符串。这样做的目的是为了在服务端的请求日志中能够通过逻辑应用名称来跟踪请求的来源，而不是只能通过IP和端口号跟进。
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaGenerator")
  //关键字的序列化类，实现以下接口： org.apache.kafka.common.serialization.Serializer 接口。
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  //值的序列化类，实现以下接口： org.apache.kafka.common.serialization.Serializer 接口。
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  val nameAddrs = Map("bob" -> "shanghai#200000", "amy" -> "beijing#100000", "alice" -> "shanghai#200000",
    "tom" -> "beijing#100000", "lulu" -> "hangzhou#310000", "nick" -> "shanghai#200000")

  val namePhones = Map("bob" -> "15700079421", "amy" -> "18700079458", "alice" -> "17730079427",
    "tom" -> "16700379451", "lulu" -> "18800074423", "nick" -> "14400033426")

  // 生成模拟数据(name, addr, type:0)
  for (nameAddr <- nameAddrs) {
    //产生消息记录
    val data = new ProducerRecord[String, String](topic, nameAddr._1, s"${nameAddr._1}\t${nameAddr._2}\t0")
    //写入kafka
    producer.send(data)

    if (rnd.nextInt(100) < 50)
      Thread.sleep(rnd.nextInt(10))
  }

  // 生成模拟数据(name, addr, type:1)
  for (namePhone <- namePhones) {
    val data = new ProducerRecord[String, String](topic, namePhone._1, s"${namePhone._1}\t${namePhone._2}\t1")
    producer.send(data)

    if (rnd.nextInt(100) < 50)
      Thread.sleep(rnd.nextInt(10))
  }

  System.out.println("sent per second: " + (nameAddrs.size + namePhones.size) * 1000 / (System.currentTimeMillis() - t))

  producer.close()
}