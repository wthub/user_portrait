package com.Parquets

import org.apache.spark.sql.{DataFrame, SparkSession}

object Cent {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local").appName("DB").config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    //读取parquet文件
    val df = ss.read.parquet("/in/part-00000-dadd3ad9-40d9-4fa5-8314-c84105e4dc93-c000.snappy.parquet")
    //Sparkcore操作
    val value = df.rdd.map(x=>((x.getAs[String]("provincename"),x.getAs[String]("cityname")),1))
    //视图
    df.createTempView("Log")
    //sparksql执行sql
    val v: DataFrame = ss.sql("select provincename,cityname,count(1) from Log group by provincename,cityname")
    v.show(10)
    //存入数据库
    //v.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"pro",JDBCUtils.getJdbcProp()._1)
    //存入分区本地
    //df.write.partitionBy("provincename","cityname").json("file:///D:\\demo\\out")





  }

}
