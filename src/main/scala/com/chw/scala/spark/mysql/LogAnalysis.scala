package com.chw.scala.spark.mysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 将 20180101.log和20180102.log通过cp命令复制到logs目录中
 */
case class Record(log_level: String, method: String, content: String)

object LogAnalysis extends App {
  val sparkConf = new SparkConf().setAppName("LogAnalysis").setMaster("local[2]")
    .set("spark.local.dir", "./tmp")
  val spark = SparkSession.builder()
    .appName("LogAnalysis")
    .config(sparkConf)
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("error")
  val ssc = new StreamingContext(sc, Seconds(2))

  // Mysql配置
  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "123456")

  // 读入日志文件目录下的日志信息流
  val logStream = ssc.textFileStream("./logs")

  //官网推荐正确用法:
  //  dstream.foreachRDD { rdd =>
  //    rdd.foreachPartition { partitionOfRecords =>
  //      val connection = createNewConnection()
  //      partitionOfRecords.foreach(record => connection.send(record))
  //      connection.close()
  //    }
  //  }

  //优化用法:
  //  dstream.foreachRDD { rdd =>
  //    rdd.foreachPartition { partitionOfRecords =>
  //      // ConnectionPool is a static, lazily initialized pool of connections
  //      val connection = ConnectionPool.getConnection()
  //      partitionOfRecords.foreach(record => connection.send(record))
  //      ConnectionPool.returnConnection(connection)  // return to the pool for future reuse
  //    }
  //  }

  logStream.foreachRDD(rdd => {
    println("driver 端执行,此处的对象,如果在executor中会使用,需要序列化,但connection是无法序列化的")

    if (!rdd.isEmpty) {
      rdd.foreachPartition(partitionOfRecords => {
        println("这里不同分区的数据,在executor 端执行")

        println("从延迟初始化的资源池中获取连接对象,连接池是静态的,惰性初始化的")
        val conn = MysqlManager.getMysqlPool.getConnection
        val statement = conn.createStatement()

        try {
          //不自动,执行批量提交
          conn.setAutoCommit(false)

          partitionOfRecords.foreach(record => {
            println(s"将数据插入到数据库中:$record")

            val tokens = record.split("\t")
            if (tokens(0) == "[error]" || tokens(0) == "[warn]") {
              //需要执行的sql操作
              val sql = s"insert into important_logs1 (log_level,method,content) values('${tokens(0)}','${tokens(1)}','${tokens(2)}')"
              //加入batch
              statement.addBatch(sql)
            }
          })

          //执行batch
          statement.executeBatch()
          //执行提交
          conn.commit()

        } catch {
          case e: Exception => {
            println(s"插入数据出现错误:$e")
          }
        } finally {
          statement.close()
          conn.close()
        }

        println("将连接对象返还到资源池中,下次重复利用")
      })
    }
  })

  // 将日志信息流转换为dataframe
  logStream.foreachRDD((rdd: RDD[String]) => {

    import spark.implicits._
    val data: DataFrame = rdd.map(w => {

      val tokens = w.split("\t")
      Record(tokens(0), tokens(1), tokens(2))
    }).toDF()

    data.createOrReplaceTempView("alldata")

    // 条件筛选
    val logImp: DataFrame = spark.sql("select * from alldata where log_level='[error]' or log_level='[warn]'")
    logImp.show()

    // 输出到外部Mysql中
    val schema = StructType(Array(StructField("log_level", StringType, true)
      , StructField("method", StringType, true)
      , StructField("content", StringType, true)))

    logImp.write.mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/spark", "important_logs", properties)

  })

  ssc.start()
  ssc.awaitTermination()
}