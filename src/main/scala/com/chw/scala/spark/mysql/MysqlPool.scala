package com.chw.scala.spark.mysql

import com.mchange.v2.c3p0.ComboPooledDataSource
import java.sql.Connection
import org.apache.log4j.Logger

/**
 * MySql通用连接池类
 */
class MysqlPool extends Serializable {
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true) //自动注册
  try {
    //设置Mysql信息
    cpds.setJdbcUrl("jdbc:mysql://localhost:3306/spark?useUnicode=true&characterEncoding=UTF-8")
    cpds.setDriverClass("com.mysql.jdbc.Driver") //对应mysql-connector-java-5.1.47版本的驱动
    cpds.setUser("root")
    cpds.setPassword("123456")
    cpds.setMaxPoolSize(200) //连接池最大连接数
    cpds.setMinPoolSize(20) //连接池最小连接数
    cpds.setAcquireIncrement(2) //连接数每次递增数量
    cpds.setMaxStatements(180) //连接池最大空闲时间
  } catch {
    case e: Exception => e.printStackTrace()
  }

  //获取连接
  def getConnection: Connection = {
    try {
      return cpds.getConnection()
    } catch {
      case e: Exception => e.printStackTrace()
        null
    }
  }
}

//惰性单例，真正计算时才初始化对象
object MysqlManager {
  @volatile private var mysqlPool: MysqlPool = _

  def getMysqlPool: MysqlPool = {
    if (mysqlPool == null) {
      synchronized {
        if (mysqlPool == null) {
          mysqlPool = new MysqlPool
        }
      }
    }
    mysqlPool
  }
}
