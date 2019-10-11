package com.chw.scala.spark.crawler.util

/**
 * @author litaoxiao
 * configuration
 */
object Conf extends Serializable {
  // mysql configuration
  val mysqlConfig = Map("url" -> "jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8", "username" -> "root", "password" -> "123456")
  val maxPoolSize = 5
  val minPoolSize = 2
}
