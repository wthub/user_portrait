package com.app

import com.Parquets.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppTag extends Tags{
  /*
  app名称标签
   */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]//转换参数类型
    val bro = args(1).asInstanceOf[Broadcast[Map[String, String]]]//转换广播变量参数
    val appname = row.getAs[String]("appname")//获取appname
    val appid = row.getAs[String]("appid")//获取appid
    if (StringUtils.isNoneBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNoneBlank(appid)){
      list:+=("APP"+bro.value.getOrElse(appid,appid),1)
    }
    list
  }
}
