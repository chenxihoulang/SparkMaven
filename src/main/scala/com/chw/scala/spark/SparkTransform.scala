package com.chw.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkTransform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")

    val sc = new SparkContext(conf)

    val nameRdd = sc.parallelize(List[(String, Int)](("zhangsan", 18), ("wangwu", 19), ("lisi", 18), ("chw", 100)), 3)
    val scoreRdd = sc.parallelize(List[(String, Int)](("zhangsan", 100), ("wangwu", 200), ("lisi", 180), ("maliu", 90)), 4)


    //    val leftJoinRdd: RDD[(String, (Int, Option[Int]))] = nameRdd.leftOuterJoin(scoreRdd)
    //    leftJoinRdd.foreach(println)
    //
    //    leftJoinRdd.foreach(one => {
    //      val name = one._1
    //      val age = one._2._1
    //      val score = one._2._2
    //
    //      println(name, age, score)
    //    })
    //
    //    val rightJoinRdd: RDD[(String, (Option[Int], Int))] = nameRdd.rightOuterJoin(scoreRdd)
    //
    //    val fullJoinRdd: RDD[(String, (Option[Int], Option[Int]))] = nameRdd.fullOuterJoin(scoreRdd)


    //    val nameScoreJoinRdd: RDD[(String, (Int, Int))] = nameRdd.join(scoreRdd)
    //    nameScoreJoinRdd.foreach(println)

    //    val partitions1: Int = nameRdd.getNumPartitions
    //
    //    val partitions2: Int = scoreRdd.getNumPartitions
    //
    //    val partitions3: Int = nameScoreJoinRdd.getNumPartitions

    //    println(partitions1, partitions2, partitions3)

    //    val unionRdd: RDD[(String, Int)] = nameRdd.union(scoreRdd)
    //
    //    unionRdd.foreach(println)
    //    println(unionRdd.getNumPartitions)


    val rdd: RDD[String] = sc.parallelize(List("hello1", "hello2", "hello3", "hello4", "hello5", "hello6"), 2)
    //    rdd.map(one => {
    //      println("建立连接...")
    //      println(s"插入数据...$one")
    //      println("关闭连接")
    //      one + "#"
    //    }).count()

//    rdd.mapPartitions(iter => {
//
//      println("建立连接...")
//      val list = ListBuffer[String]()
//      while (iter.hasNext) {
//        val item = iter.next()
//        list += item
//
//        println(s"插入数据...$item")
//      }
//
//      println("关闭连接")
//
//      list.iterator
//    }, false)
//      .count()

    rdd.foreachPartition(iter => {
      println("建立连接...")
      val list = ListBuffer[String]()
      while (iter.hasNext) {
        val item = iter.next()
        list += item

        println(s"插入数据...$item")
      }

      println("关闭连接")
    })

  }
}
