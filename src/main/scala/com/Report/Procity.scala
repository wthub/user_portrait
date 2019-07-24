package com.Report

import com.config.ConfigManager
import com.parqduetUtils.{JDBCUtils, SparkSsUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object Procity {
  def main(args: Array[String]): Unit = {
    //获取执行对象
    val ss: SparkSession = SparkSsUtils.getSarksess()
    //读取文件
    val df = ss.read.parquet("/in/part-00000-dadd3ad9-40d9-4fa5-8314-c84105e4dc93-c000.snappy.parquet")
    //创建临时表
    df.createTempView("test")
    //加载SQL文件
    val sql: String = ConfigManager.getProper("pro_city")
    val device: String = ConfigManager.getProper("device")
    val system: String = ConfigManager.getProper("system")
    if(sql==null){//加载不到答应logger
      LoggerFactory.getLogger("SparkLogger")
    }else{//加载到执行sql得到DF
      val df1 = ss.sql(sql)
      //存入mysql数据库
      df1.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"pro_city",JDBCUtils.getJdbcProp()._1)
    }
    val dev = ss.sql(device)
    val sys = ss.sql(system)
    dev.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"dev",JDBCUtils.getJdbcProp()._1)
    sys.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"system",JDBCUtils.getJdbcProp()._1)
  }

}
