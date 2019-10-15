package com.chw.scala.spark.hbase

import java.io.IOException

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * hbase中增删改查数据
 */
object HbaseUtils {
  val configuration = HBaseConfiguration.create()
  //hbase架构中,client是直接和zookeeper通信的,所以这里需要配置zookeeper集群地址
  configuration.set(HConstants.ZOOKEEPER_QUORUM, "hadoop07,hadoop08,hadoop09")
  configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "")

  //建立连接
  val connection: Connection = ConnectionFactory.createConnection(configuration)
  //获取admin
  val admin: Admin = connection.getAdmin

  /**
   * 判断表是否存在
   *
   * @param tableName 表名
   */
  def isExists(tableName: String): Boolean = {
    val tName = TableName.valueOf(tableName)

    admin.tableExists(tName)
  }

  /**
   * 创建一个hbase表
   *
   * @param tableName     表名
   * @param columnFamilys 列族
   */
  def createTable(tableName: String, columnFamilys: Array[String]) = {
    //操作的表名
    val tName = TableName.valueOf(tableName)

    //当表不存在的时候创建Hbase表
    if (!admin.tableExists(tName)) {
      //创建Hbase表模式
      val descriptor = new HTableDescriptor(tName)

      //创建列簇i
      for (columnFamily <- columnFamilys) {
        descriptor.addFamily(new HColumnDescriptor(columnFamily))
      }

      //创建表
      admin.createTable(descriptor)
      println(s"创建表$tableName 成功")
    }
  }

  /**
   * 删除一个Hbase表
   *
   * @param tableName 表名
   */
  def dropTable(tableName: String): Unit = {
    //先禁用表
    admin.disableTable(TableName.valueOf(tableName))
    //再删除表
    admin.deleteTable(TableName.valueOf(tableName))
    println(s"删除表$tableName 成功")
  }

  /**
   * 向hbase表中插入数据,put 'sk:test1','1','i:name','Luck2'
   *
   * @param tableName    表名
   * @param rowkey       行键
   * @param columnFamily 列族
   * @param column       列名
   * @param value        列值
   */
  def insertTable(tableName: String, rowkey: String, columnFamily: String, column: String, value: String) = {
    //确定需要添加数据的表
    val table = connection.getTable(TableName.valueOf(tableName))
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    //添加列簇名,字段名,字段值value
    puts.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())
    //把数据插入到table中
    table.put(puts)
    println("插入数据成功")
  }

  /**
   * 查询指定cell中的值
   */
  def getData(tableName: String, rowkey: String, columnFamily: String, column: String): String = {
    val table = connection.getTable(TableName.valueOf(tableName))

    //通过rowkey指定行
    val get = new Get(rowkey.getBytes)
    get.addFamily(columnFamily.getBytes)
    val rs: Result = table.get(get)

    //指定列名,确定cell,用于获取cell中最新timestamp对应的值
    val cell = rs.getColumnLatestCell(columnFamily.getBytes, column.getBytes)

    //获取cell的rowkey的值
    println(Bytes.toString(CellUtil.cloneRow(cell)))

    //获取cell的value值
    val value = Bytes.toString(CellUtil.cloneValue(cell))

    value
  }

  /**
   * 获取hbase表中的数据,scan 'sk:test1'
   *
   * @param tableName    表名
   * @param columnFamily 列族
   * @param column       列名
   */
  def scanDataFromHTable(tableName: String, columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //定义scan对象
    val scan = new Scan()
    //添加列簇名称
    scan.addFamily(columnFamily.getBytes())
    //从table中抓取数据来scan
    val scanner: ResultScanner = table.getScanner(scan)

    var result: Result = scanner.next()
    //数据不为空时输出数据
    while (result != null) {
      val cell = result.getColumnLatestCell(columnFamily.getBytes, column.getBytes)
      println(Bytes.toString(CellUtil.cloneRow(cell)))
      println(Bytes.toString(CellUtil.cloneValue(cell)))

      println(s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column}," +
        s"value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
  }

  /**
   * 获取全表数据
   *
   * @param tableName 表名
   */
  def getAllData(tableName: String): ListBuffer[String] = {
    var table: Table = null
    val list = new ListBuffer[String]
    try {
      table = connection.getTable(TableName.valueOf(tableName))
      val results: ResultScanner = table.getScanner(new Scan)

      import scala.collection.JavaConversions._
      for (result <- results) {
        for (cell <- result.rawCells) {
          val row: String = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
          val family: String = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val colName: String = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)

          val context: String = "rowkey:" + row + "," + "列族:" + family + "," + "列:" + colName + "," + "值:" + value
          list += context
        }
      }

      results.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }

    list
  }

  /**
   * 删除某条记录,delete 'sk:test1','1','i:name'
   *
   * @param tableName    表名
   * @param rowkey       行键
   * @param columnFamily 列族
   * @param column       列名
   */
  def deleteRecord(tableName: String, rowkey: String, columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))

    val info = new Delete(Bytes.toBytes(rowkey))
    info.addColumn(columnFamily.getBytes(), column.getBytes())
    table.delete(info)
    println("删除成功")
  }

  /**
   * 关闭 connection 连接
   */
  def close() = {
    if (connection != null) {
      try {
        connection.close()
        println("关闭成功")
      } catch {
        case e: IOException => println("关闭失败")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    /*var arr=new Array[String](1)
    arr(0)="info1"
    createTable("user2",arr)*/
    //insertTable("user2","1","info1","name","lyh")
    //scanDataFromHTable("user2","info1","name")
    //deleteRecord("user2","1","info1","name")
    dropTable("user2")
    println(isExists("user2"))
    close()
  }
}
