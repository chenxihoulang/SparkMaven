package com.chw.scala.spark.word_freq_kafka_mysql.dao

import java.sql.Connection
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.chw.scala.spark.word_freq_kafka_mysql.util.Conf
import org.apache.log4j.LogManager

/**
 * Mysql连接池类(c3p0)
 *
 * @author litaoxiao
 *
 */
class MysqlPool extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val conf = Conf.mysqlConfig

  try {
    //利用c3p0设置MySql配置
    cpds.setJdbcUrl(conf.get("url").getOrElse("jdbc:mysql://localhost:3306/spark?useUnicode=true&amp;characterEncoding=UTF-8"));
    cpds.setDriverClass("com.mysql.jdbc.Driver");
    cpds.setUser(conf.get("username").getOrElse("root"));
    cpds.setPassword(conf.get("password").getOrElse("123456"))
    //初始连接数
    cpds.setInitialPoolSize(3)
    //最大连接数
    cpds.setMaxPoolSize(Conf.maxPoolSize)
    //最小连接数
    cpds.setMinPoolSize(Conf.minPoolSize)
    //递增步长
    cpds.setAcquireIncrement(5)
    //最大空闲时间
    cpds.setMaxStatements(180)
    /* 最大空闲时间,25000秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0 */
    cpds.setMaxIdleTime(25000)
    // 检测连接配置
    cpds.setPreferredTestQuery("select id from user_words limit 1")
    cpds.setIdleConnectionTestPeriod(18000)
  } catch {
    case e: Exception =>
      log.error("[MysqlPoolError]", e)
  }

  //从连接池获取连接
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case e: Exception =>
        log.error("[MysqlPoolGetConnectionError]", e)
        null
    }
  }
}

object MysqlManager {
  var mysqlManager: MysqlPool = _

  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}