package com.parqduetUtils

import org.apache.spark.sql.SparkSession

object SparkSsUtils {
  //获取SparkSession对象
  def getSarksess():SparkSession={
    val spark = SparkSession.builder().master("local").appName("DB").config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    return spark
  }
}
