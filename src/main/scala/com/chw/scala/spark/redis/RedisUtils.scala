package com.chw.scala.spark.redis

import redis.clients.jedis.Jedis

object RedisUtils extends Serializable {
  var readJedis: Jedis = _
  var writeJedis: Jedis = _

  //验证Redis连接
  def checkAlive {
    //判断连接是否可用
    if (!isConnected(readJedis)) {
      readJedis = reConnect(readJedis)
    }

    if (!isConnected(writeJedis)) {
      writeJedis = reConnect(writeJedis)
    }
  }

  //获取Redis连接
  def getConn(ip: String, port: Int, passwd: String) = {
    val jedis = new Jedis(ip, port, 5000)
    jedis.connect

    if (passwd.length > 0) {
      jedis.auth(passwd)
    }

    jedis
  }

  //查看连接是否可用
  def isConnected(jedis: Jedis) = jedis != null && jedis.isConnected

  //重新连接
  def reConnect(jedis: Jedis) = {
    println("重新连接...")
    disConnect(jedis)
    getConn("hadoop06", 6379, "")
  }

  //释放连接
  def disConnect(jedis: Jedis): Unit = {
    if (jedis != null && jedis.isConnected) {
      jedis.close
    }
  }
}
