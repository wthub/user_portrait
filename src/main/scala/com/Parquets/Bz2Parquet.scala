package com.Parquets

import com.Constants.SchemaUtils
import com.parqduetUtils.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Bz2Parquet {

  def main(args: Array[String]): Unit = {
    if(args.length !=2) {
      println("参数不正确")
      sys.exit()
    }
    val ss = SparkSession.builder().appName("BZ").master("local").config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
      //设置序列化类型

    //读取log文件
    val lines: RDD[String] = ss.sparkContext.textFile(args(0))
    val l = lines.map(_.split(",",-1)).filter(_.length>=85)
      .map(arr=>{
        Row(
        arr(0),
        StringUtils.stringTint(arr(1)),
        StringUtils.stringTint(arr(2)),
        StringUtils.stringTint(arr(3)),
        StringUtils.stringTint(arr(4)),
        arr(5),
        arr(6),
        StringUtils.stringTint(arr(7)),
        StringUtils.stringTint(arr(8)),
        StringUtils.stringTdouble(arr(9)),
        StringUtils.stringTdouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringUtils.stringTint(arr(17)),
        arr(18),
        arr(19),
        StringUtils.stringTint(arr(20)),
        StringUtils.stringTint(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringUtils.stringTint(arr(26)),
        arr(27),
        StringUtils.stringTint(arr(28)),
        arr(29),
        StringUtils.stringTint(arr(30)),
        StringUtils.stringTint(arr(31)),
        StringUtils.stringTint(arr(32)),
        arr(33),
        StringUtils.stringTint(arr(34)),
        StringUtils.stringTint(arr(35)),
        StringUtils.stringTint(arr(36)),
        arr(37),
        StringUtils.stringTint(arr(38)),
        StringUtils.stringTint(arr(39)),
        StringUtils.stringTdouble(arr(40)),
        StringUtils.stringTdouble(arr(41)),
        StringUtils.stringTint(arr(42)),
        arr(43),
        StringUtils.stringTdouble(arr(44)),
        StringUtils.stringTdouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringUtils.stringTint(arr(57)),
        StringUtils.stringTdouble(arr(58)),
        StringUtils.stringTint(arr(59)),
        StringUtils.stringTint(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringUtils.stringTint(arr(73)),
        StringUtils.stringTdouble(arr(74)),
        StringUtils.stringTdouble(arr(75)),
        StringUtils.stringTdouble(arr(76)),
        StringUtils.stringTdouble(arr(77)),
        StringUtils.stringTdouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringUtils.stringTint(arr(84))
        )
      })
    // 构建DF
    val df = ss.sqlContext.createDataFrame(l,SchemaUtils.LogStructType)
    // 存储到指定位置
    df.write.parquet(args(1))
    ss.stop()

  }

}
