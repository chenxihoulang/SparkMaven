package com.chw.scala.spark.kafka

import com.chw.scala.spark.kafka.Producer.props
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaOperation extends App {
  //KafkaConsumer is not safe for multi-threaded access,在本地运行的话,此处需要setMaster("local")
  val sparkConf = new SparkConf().setAppName("KafkaOperation").setMaster("local")
    .set("spark.local.dir", "./tmp")
    .set("spark.streaming.kafka.maxRatePerPartition", "10")

  //创建流式上下文,2s为批处理间隔
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  ssc.sparkContext.setLogLevel("error")

  // Create direct kafka stream with brokers and topics
  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9091,localhost:9092", //服务器地址
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer], //序列化类型
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> "kafkaOperationGroup", //group设置,消费者所属的组
    //发出请求时传递给服务器的 ID 字符串。这样做的目的是为了在服务端的请求日志中能够通过逻辑应用名称来跟踪请求的来源，而不是只能通过IP和端口号跟进。
    ConsumerConfig.CLIENT_ID_CONFIG -> "kafkaGenerator",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest", //从最新的offset开始
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)) //不自动提交

  //根据broker和topic创建直接通过kafka连接Driect Kafka
  val kafkaDirectStream: InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](List("kafkaOperation"), kafkaParams))

  //切分用户姓名和地址
  val nameAddrStream: DStream[(String, String)] = kafkaDirectStream.map(_.value).filter(record => {
    val tokens = record.split("\t")
    // addr type 0
    tokens(2).toInt == 0
  }).map(record => {
    val tokens = record.split("\t")
    (tokens(0), tokens(1))
  })

  //切分用户姓名和电话
  val namePhoneStream: DStream[(String, String)] = kafkaDirectStream.map(_.value).filter(record => {
    val tokens = record.split("\t")
    // phone type 1
    tokens(2).toInt == 1
  }).map(record => {
    val tokens = record.split("\t")
    (tokens(0), tokens(1))
  })

  //将用户姓名,地址与电话进行合并
  val nameAddrPhoneStream: DStream[String] = nameAddrStream.join(namePhoneStream).map(record => {
    s"姓名：${record._1}, 地址：${record._2._1}, 电话：${record._2._2}"
  })

  nameAddrPhoneStream.print()

  // 开始运算
  ssc.start()
  ssc.awaitTermination()
}