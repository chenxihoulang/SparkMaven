package com.chw.scala.spark.userbehavior.main

import com.chw.scala.spark.userbehavior.util.{Conf, Tool}

import scala.collection.mutable.ArrayBuffer

trait RealStrategy extends Serializable {
  val TRIM_DURATION = Conf.EXPIRE_DURATION

  def getKeyFields: Array[Int]
  def update(log: Seq[Array[String]], previous: ArrayBuffer[Long]): ArrayBuffer[Long]
  def trim(timestamps: ArrayBuffer[Long]) = trimHelper(timestamps, TRIM_DURATION)

  def trimHelper(timestamps: ArrayBuffer[Long], duration: Long) = {
    var i = 0
    while (i < timestamps.length && Tool.isInvalidate(timestamps(i), duration))
      i += 1
    timestamps.slice(i, timestamps.length)
  }

}