package com.atguigu.sparkmall.mock.util

/**
  * Created by qzy017 on 2019/6/12.
  */
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory

object JDBCUtil {

  val dataSource = initConnection()

  /**
    * 初始化的连接
    */
  def initConnection() = {
    val properties = new Properties()
    val config = ConfigurationUtil("config.properties")
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getString("jdbc.url"))
    properties.setProperty("username", config.getString("jdbc.user"))
    properties.setProperty("password", config.getString("jdbc.password"))
    properties.setProperty("maxActive", config.getString("jdbc.maxActive"))
    DruidDataSourceFactory.createDataSource(properties)
  }
  /**
    * 执行单条语句
    *
    * "insert into xxx values (?,?,?)"
    */
  def executeUpdate(sql: String, args: Array[Any]) = {
    val conn = dataSource.getConnection
    conn.setAutoCommit(false)
    val ps = conn.prepareStatement(sql)
    if (args != null && args.length > 0) {
      (0 until args.length).foreach {
        i => ps.setObject(i + 1, args(i))
      }
    }
    ps.executeUpdate
    conn.commit()
  }

  /**
    * 执行批处理
    */
  def executeBatchUpdate(sql: String, argsList: Iterable[Array[Any]]) = {
    val conn = dataSource.getConnection
    conn.setAutoCommit(false)
    val ps = conn.prepareStatement(sql)
    argsList.foreach {
      case args: Array[Any] => {
        (0 until args.length).foreach {
          i => ps.setObject(i + 1, args(i))
        }
        ps.addBatch()
      }
    }
    ps.executeBatch()
    conn.commit()
  }




  def main(args: Array[String]): Unit = {

      var sql="INSERT INTO `sparkmall`.`category_top10` (`taskId`,`category_id`,`click_count`,`order_count`,`pay_count`) VALUES(?,?,?,?,?)"

  var array=Array("taskId", "category_id", 72, 22, 33)

    var array1=Array("taskId", "category_id", 52, 22, 33)


    var set=Set(array,array1)
    executeBatchUpdate(sql,set)
  }
}