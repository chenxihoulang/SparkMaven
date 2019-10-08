package com.chw.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object transformation_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("word_count")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    //    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5, 3), 2)


    //    rdd.mapPartitions(iter => {
    //      println("a")
    //
    //      iter.foreach(print)
    //      iter
    //    }).collect().foreach(println)

    //    rdd.mapPartitionsWithIndex((index, iter) => {
    //      println(index)
    //      iter
    //    }).collect()

    //    rdd.sample(false, 0.5).foreach(println)

    //    val rdd = sc.parallelize(Array(("a", 1), ("a", 2), ("c", 3)), 2)
    //
    //    val unit: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    //
    //    unit.foreach(println)
    //
    //    rdd.reduceByKey(_ + _).foreach(println)
    //
    //    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 3))
    //
    //    rdd1.distinct(2).foreach(println)

    val rdd2 = sc.parallelize(Array(("v1", 1), ("v2", 1), ("u1", 2)), 2)
    val rdd3 = sc.parallelize(Array(("v1", 3), ("u1", 2), ("u2", 2), ("v2", 4), ("v1", 1)), 2)

    //    rdd2.join(rdd3).foreach(println)
    //    rdd2.union(rdd3).foreach(println)

    //    rdd2.cogroup(rdd3).foreach(println)

    val rdd4 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8), 2)
    //    val rdd5 = sc.parallelize(Array(4, 5))
    //    rdd4.cartesian(rdd5).foreach(println)


    //    rdd2.sortByKey().foreach(println)

    //    rdd3.repartition(4).foreach(println)

    //    rdd3.countByKey().foreach(println)

    val rdd6: RDD[Int] = rdd4.map(_ * 10)

    //    println(rdd6.reduce(_ * _))

    val ints: Array[Int] = rdd6.takeOrdered(2)
    ints.foreach(println)

    //避免重复计算
    rdd6.cache()
    rdd6.unpersist()

    val sumAccumulator = sc.longAccumulator("sum")

    rdd6.foreach(x => sumAccumulator.add(x))

    println(sumAccumulator.value)
  }
}


