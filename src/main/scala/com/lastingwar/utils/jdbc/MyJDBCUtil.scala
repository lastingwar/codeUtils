package com.lastingwar.utils.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.alibaba.fastjson.JSONObject
import com.lastingwar.utils.PropertiesUtil
import javax.sql.DataSource

import scala.collection.mutable.ListBuffer


/**
 * JDBC连接mysql工具类
 * @author yhm
 * @create 2020-11-02 14:46
 */
object MyJDBCUtil {

    //初始化连接池
    var dataSource: DataSource = init()

    //初始化连接池方法
    def init(): DataSource = {

      val properties = new Properties()
      val config: Properties = PropertiesUtil.load("config.properties")

      // 配置选择config.properties 文件
      properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
      properties.setProperty("url", config.getProperty("jdbc.url"))
      properties.setProperty("username", config.getProperty("jdbc.user"))
      properties.setProperty("password", config.getProperty("jdbc.password"))
      properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))

      DruidDataSourceFactory.createDataSource(properties)
    }

    //获取MySQL连接
    def getConnection: Connection = {
      dataSource.getConnection
    }

    //执行SQL语句,单条数据插入
    def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {

      var rtn = 0
      var pstmt: PreparedStatement = null

      try {
        connection.setAutoCommit(false)
        pstmt = connection.prepareStatement(sql)

        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i))
          }
        }

        rtn = pstmt.executeUpdate()

        connection.commit()
        pstmt.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }

      rtn
    }

    //执行SQL语句,批量数据插入
    def executeBatchUpdate(connection: Connection, sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {

      var rtn: Array[Int] = null
      var pstmt: PreparedStatement = null

      try {
        connection.setAutoCommit(false)
        pstmt = connection.prepareStatement(sql)

        for (params <- paramsList) {

          if (params != null && params.length > 0) {

            for (i <- params.indices) {
              pstmt.setObject(i + 1, params(i))
            }

            pstmt.addBatch()
          }
        }

        rtn = pstmt.executeBatch()

        connection.commit()
        pstmt.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }

      rtn
    }

    //判断一条数据是否存在
    def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {

      var flag: Boolean = false
      var pstmt: PreparedStatement = null

      try {
        pstmt = connection.prepareStatement(sql)

        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }

        flag = pstmt.executeQuery().next()
        pstmt.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }

      flag
    }

    //获取MySQL的一条数据
    def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {

      var result: Long = 0L
      var pstmt: PreparedStatement = null

      try {
        pstmt = connection.prepareStatement(sql)

        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }

        val resultSet: ResultSet = pstmt.executeQuery()

        while (resultSet.next()) {
          result = resultSet.getLong(1)
        }

        resultSet.close()
        pstmt.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }

      result
    }

  //获取MySQL中的黑名单数据
  def getBlackListFromMysql(connection: Connection, sql: String, params: Array[Any]): List[String] = {
    var result: ListBuffer[String] = new ListBuffer[String]()
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result += resultSet.getString(1)
      }
      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result.toList
  }

  //读取MySQL数据,封装为JSON字符串
  def getUserInfoFromMysql(connection: Connection, sql: String, params: Array[Any]): String = {
    val userJsonObj = new JSONObject()

    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }

      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        userJsonObj.put("id", resultSet.getString("id"))
        userJsonObj.put("gender",resultSet.getString("gender"))
        userJsonObj.put("user_level",resultSet.getString("user_level"))
        userJsonObj.put("birthday",resultSet.getString("birthday"))
      }

      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    userJsonObj.toString
  }
    //主方法,用于测试上述方法
    def main(args: Array[String]): Unit = {

      //1 获取连接
      val connection: Connection = getConnection

      //2 预编译SQL
      val sql = "select * from user_info where id > ?"
      val statement: PreparedStatement = connection.prepareStatement(sql)

      //3 传输参数
      statement.setObject(1, 2)

      //4 执行sql
      val resultSet: ResultSet = statement.executeQuery()

      //5 获取数据
      while (resultSet.next()) {
        println("111:" + resultSet.getString(2))
      }

      //6 关闭资源
      resultSet.close()
      statement.close()
      connection.close()
    }
}
