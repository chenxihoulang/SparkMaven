package com.chw.scala.spark.kafak_behavior_data_producer

import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._

class ConsumerTest(val brokers: String,
                   val groupId: String,
                   val topic: String) extends Logging {

  val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)

  def shutdown() = {
    if (consumer != null)
      consumer.close();
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "userBehaviorConsumer")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") //周期性的自动提交consumer的offset
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000") //提交offset周期为1000ms
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)

          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}

object ConsumerTest extends App {
  val brokers = "localhost:9091,localhost:9092"
  val groupId = "group1"
  val topic = "userBehavior"

  val example = new ConsumerTest(brokers, groupId, topic)
  example.run()
}