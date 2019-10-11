package com.chw.scala.spark.crawler.main
import java.util.Properties

import scala.util.Random

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import spray.json._
import DefaultJsonProtocol._

object Producer extends App {
  val pageNumPerGame = 20
  val topic = "gameTopic"
  val brokers = "localhost:9091,localhost:9092"

  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "monitorAlarmCrawler")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val startTime = System.currentTimeMillis()

  // 从taptap上爬取用户评论数据(game_id, comments)
  val crawlerData = Crawler.crawData(pageNumPerGame)

  var events = 0
  for (e <- crawlerData) {
    val (game_id, reviews) = e
    reviews.foreach(review => {
      //将评论数据进行UTF-8编码后再发送到kafka
      val revUtf8 = new String(review.getBytes, 0, review.length, "UTF8")

      //隐式转换,将map转换为json后,发送到kafka
      val data = new ProducerRecord[String, String](topic, game_id.toString,
        Map("gameId" -> game_id.toString, "review" -> revUtf8).toJson.toString)

      producer.send(data)

      events += 1
    })
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - startTime))
  producer.close()
}