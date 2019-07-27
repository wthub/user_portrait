package com.app

import com.Parquets.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AreaTag extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val rtbprovince = row.getAs[String]("rtbprovince")
    val rtbcity = row.getAs[String]("rtbcity")
    //省份标签
    if (StringUtils.isNoneBlank(rtbprovince)){
      list:+=("ZP"+rtbprovince,1)
    }
    //城市标签
    if (StringUtils.isNoneBlank(rtbcity)){
      list:+=("ZC"+rtbcity,1)
    }
    list
  }
}
