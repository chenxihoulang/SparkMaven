package com.chw.scala.spark.alarm.service

import spray.json._
import com.chw.scala.spark.alarm.entity.Record

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val docFormat = jsonFormat2(Record)
}

/**
 * 类与json字符串的互相转换
 */
object JsonParse {
  import spray.json._
  import MyJsonProtocol._

  def record2Json(doc: Record): String = {
    //通过 spray.json.enrichAny隐式转换为RichAny
    //RichAny中的toJson需要的隐式参数JsonWriter,来自于MyJsonProtocol.docFormat隐式值
    doc.toJson.toString()
  }

  def json2Record(json: String): Record = {
    //通过spray.json.enrichString隐式转换为RichString
    json.parseJson.convertTo[Record]
  }

}