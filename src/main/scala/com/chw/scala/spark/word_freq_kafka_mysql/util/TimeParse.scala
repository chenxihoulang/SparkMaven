package com.chw.scala.spark.word_freq_kafka_mysql.util

import org.joda.time._

object TimeParse extends Serializable {
  def timeStamp2String(timeStamp: String, format: String): String = {
    val ts = timeStamp.toLong * 1000;
    new DateTime(ts).toDateTime.toString(format)
  }

  def timeStamp2String(timeStamp: Long, format: String): String = {
    new DateTime(timeStamp).toDateTime.toString(format)
  }
}