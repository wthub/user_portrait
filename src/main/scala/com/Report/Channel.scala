package com.Report

import com.parqduetUtils.{JDBCUtils, SparkSsUtils, appUtils}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SaveMode


object Channel {
  def main(args: Array[String]): Unit = {
    val ss = SparkSsUtils.getSarksess()
    val dict = ss.sparkContext.textFile("file:///D:\\A资料\\项目\\项目（二）03\\Spark用户画像分析\\app_dict.txt")
    val map = dict.map(_.split("\t", -1)).filter(_.length >= 5)
      .map(x => (x(1), x(4))).collect().toMap
    import ss.implicits._
    val bro: Broadcast[Map[String, String]] = ss.sparkContext.broadcast(map)
    val df = ss.read.parquet("/in/part-00000-dadd3ad9-40d9-4fa5-8314-c84105e4dc93-c000.snappy.parquet")
    val df1 = df.map(row => {
      var appname = row.getAs[String]("appname")
      if (StringUtils.isBlank(appname)) {
        appname = bro.value.getOrElse(row.getAs[String]("appid"), "unknow")
      }
      val ispname = row.getAs[String]("ispname")
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val iswin = row.getAs[Int]("iswin")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val isbid = row.getAs[Int]("isbid")
      val adorderid = row.getAs[Int]("adorderid")
      val re_list = appUtils.request(requestmode, processnode)
      val bi_list = appUtils.Bidding(iseffective, isbilling, isbid, iswin, adorderid, adpayment, winprice)
      val cl_list = appUtils.Click(requestmode, iseffective)

      (appname, re_list ++ cl_list ++ bi_list)
    }).rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(x => x._1 + x._2)
    }).map(x => {
      (x._1, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(7), x._2(8), x._2(5), x._2(6))
    }).toDF( "appname", "original", "effective", "ad", "bidd", "scee", "show", "click", "d1", "d2")
    df1.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"Channel",JDBCUtils.getJdbcProp()._1)

  }
}
