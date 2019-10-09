package com.chw.scala.spark.performance_tuning

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
  * 通过包装器在DStream的foeachRDD中更新广播变量,避免产生序列化问题
  */
case class BroadcastWrapper[T:ClassTag](
 @transient private val ssc:StreamingContext,
 @transient private val _v:T) {
  @transient private var v=ssc.sparkContext.broadcast(_v)

  def update(newValue:T,blocking:Boolean=false)={
    //删除RDD是否需要锁定
    v.unpersist(blocking)
    v=ssc.sparkContext.broadcast(newValue)
  }

  def value:T=v.value

  private def writeObject(out:ObjectOutputStream)={
    out.writeObject(v)
  }

  private def readObject(in:ObjectInputStream)={
    v=in.readObject().asInstanceOf[Broadcast[T]]
  }
}
