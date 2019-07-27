package com.app

import com.Parquets.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AdTags extends Tags{
  /*
  广告类型位标签
   */
  override def makeTags(args: Any*): List[(String,Int)] = {
    var  list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val adtype = row.getAs[Int]("adspacetype")
    adtype match {
      case x if x >9 => list:+=("LC"+x,1)
      case x if 0<x && x<=9 =>list:+=("LC0"+x,1)
    }

    val adname = row.getAs[String]("adspacetypename")
    if (StringUtils.isNoneBlank(adname)){
      list:+=("LN"+adname,1)
    }
    //渠道标签
    val adfID = row.getAs[Int]("adplatformproviderid")

      list:+=("CN"+adfID,1)


    list

  }
}
