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
    }else{//加载到执行地域指标sql得到DF
      val df1 = ss.sql(sql)
      //地域报表存入mysql数据库
      df1.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"pro_city",JDBCUtils.getJdbcProp()._1)
    }
    //加载到执行设备指标sql得到DF
    val dev = ss.sql(device)
    //加载到执行系统指标sql得到DF
    val sys = ss.sql(system)
    //设备报表存入mysql数据库
    dev.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"dev",JDBCUtils.getJdbcProp()._1)
    //系统报表存入mysql数据库
    sys.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"system",JDBCUtils.getJdbcProp()._1)
  }

}
