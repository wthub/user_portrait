package com.parqduetUtils

import java.sql.{Connection, DriverManager}
import java.util
import java.util.Properties

import com.config.ConfigManager

object JDBCUtils {
  private val max = 50//最大连接数
  private val connectionNum = 10//每次产生连接数
  private val pool = new util.LinkedList[Connection]()//连接池
  private var conNum = 0//当前连接数



  //加载JDBC的properties文件
  def getJdbcProp():(Properties,String)={
    val prop = new Properties()
    prop.put("driver",ConfigManager.getProper("jdbc.driver"))
    prop.put("user",ConfigManager.getProper("jdbc.user"))
    prop.put("password",ConfigManager.getProper("jdbc.password"))
    val jdbcUrl = ConfigManager.getProper("jdbc.url")
    (prop,jdbcUrl)


  }


  //获取连接
  def getConnections():Connection={
    //同步代码块
    AnyRef.synchronized({
      if (pool.isEmpty){//判断连接池是否为空
        //加载驱动
        preGetconn()
        for(i <- 0 to connectionNum) {
          val conn = DriverManager.getConnection(JDBCUtils.getJdbcProp()._2,JDBCUtils.getJdbcProp()._1)

          pool.push(conn)
          conNum += 1
        }
      }
      //获取并移除此列表pool的头
      pool.poll()
    })
  }

  //释放连接
  def resultConn(conn:Connection):Unit={
    //将连接归还到列表pool中
    pool.push(conn)
  }

  def preGetconn():Unit={
    //控制驱动
    if (conNum > max){
      println("无连接")
      Thread.sleep(2000)
      preGetconn()
    }else{
      Class.forName("com.mysql.jdbc.Driver")
    }
  }

}
