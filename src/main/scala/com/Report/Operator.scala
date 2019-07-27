package com.Report

import com.parqduetUtils.{JDBCUtils, SparkSsUtils, appUtils}
import org.apache.spark.sql.{DataFrame, SaveMode}

object Operator {
  def main(args: Array[String]): Unit = {
    //获取执行对象
    val ss = SparkSsUtils.getSarksess()
    //读取文件
    val df = ss.read.parquet("/in/part-00000-dadd3ad9-40d9-4fa5-8314-c84105e4dc93-c000.snappy.parquet")
    import ss.implicits._
    val df1: DataFrame = df.map(row => {
      //获取相应数据
      val ispid = row.getAs[Int]("ispid")
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
      //以运营商、运营商id为K，list数据为V
      ((ispid, ispname), re_list ++ cl_list ++ bi_list)
    }).rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(x => {
      (x._1._1, x._1._2, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(7), x._2(8), x._2(5), x._2(6))
    })
      .toDF("ispid", "ispname", "original", "effective", "ad", "bidd", "scee", "show", "click", "d1", "d2")

    df1.write.mode(SaveMode.Ignore).jdbc(JDBCUtils.getJdbcProp()._2,"operator",JDBCUtils.getJdbcProp()._1)

  }

}
