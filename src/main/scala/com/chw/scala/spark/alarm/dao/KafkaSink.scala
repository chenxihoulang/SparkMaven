package com.chw.scala.spark.alarm.dao

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
 * 向kafka中发送数据的类
 *
 * @param createProducer 创建KafkaProducer
 * @tparam K 发送数据key的类型
 * @tparam V 数据的类型
 */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

  /**
   * 通过这种方式,能够解决KafkaProducer不能序列化,并广播的问题
   */
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)

      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }

      producer
    }

    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}
  