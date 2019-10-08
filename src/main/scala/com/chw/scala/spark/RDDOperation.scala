package com.chw.scala.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RDDOperation extends App {
  val conf = new SparkConf()
    .setAppName("rddOperation")
    .setMaster("local[2]")
  // Create spark context
  val sc = new SparkContext(conf)

  val txtNameAddr = "name_addr.txt"
  val txtNamePhone = "name_phone.txt"

  private val rddNameAddr: RDD[(String, String)] = sc.textFile(txtNameAddr).map(record => {
    val tokens = record.split(" ")
    (tokens(0), tokens(1))
  }) // rdd1
  rddNameAddr.cache

  private val rddNamePhone: RDD[(String, String)] = sc.textFile(txtNamePhone).map(record => {
    val tokens = record.split(" ")
    (tokens(0), tokens(1))
  }) // rdd2
  rddNamePhone.cache

  private val rddNameAddrPhone: RDD[(String, (String, String))] = rddNameAddr.join(rddNamePhone) // rdd3

  private val rddHtml: RDD[String] = rddNameAddrPhone.map(record => {
    val name = record._1
    val addr = record._2._1
    val phone = record._2._2
    s"<h2>姓名：${name}</h2><p>地址：${addr}</p><p>电话：${phone}</p>"
  }) // rdd4
  val rddOutput = rddHtml.saveAsTextFile("UserInfo")

  private val rddPostcode: RDD[(String, Int)] = rddNameAddr.map(record => {
    val postcode = record._2.split("#")(1)
    (postcode, 1)
  }) // rdd5
  val rddPostcodeCount = rddPostcode.reduceByKey(_ + _) // rdd6
  rddPostcodeCount.collect().foreach(println)

  sc.stop
}