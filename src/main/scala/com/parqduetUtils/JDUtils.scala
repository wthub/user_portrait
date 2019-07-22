package com.parqduetUtils

import java.sql.{Connection, DriverManager}
import java.util


object JDUtils {

  private val max = 50//最大连接数
  private val connectionNum = 10//每次产生连接数
  private val pool = new util.LinkedList[Connection]()//连接池
  private var conNum = 0//当前连接数

  //获取连接
  def getConnections():Connection={
    //同步代码块
    AnyRef.synchronized({
      if (pool.isEmpty){//判断连接池是否为空
        //加载驱动
        preGetconn()
        for(i <- 0 to connectionNum) {
          val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/charging","root","123456")
          pool.push(conn)
          conNum += 1
        }
      }
      pool.poll()
    })
  }

  //释放连接
  def resultConn(conn:Connection):Unit={
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
