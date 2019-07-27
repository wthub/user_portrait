package com.app

import com.Parquets.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object DevTag extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val map: Map[Int,String] = Map[Int,String]((1,"Android"),( 2,"IOS"),(3,"WinPhone"))
    val row = args(0).asInstanceOf[Row]

    //操作系统标签
    val client = row.getAs[Int]("client")
    if(client>0 && client<=3){
      list:+=(map.getOrElse(client,0)+" D0001000"+client,1)
    }else{
      list:+=("其他 D00010004",1)
    }
    //network网络类型标签
    val networkmannername = row.getAs[String]("networkmannername")
    val networkmannerid = row.getAs[Int]("networkmannerid")
    networkmannername match {
      case "Wifi" => list:+=("WIFI D00020001",1)
      case "4G" => list:+=("4G D00020002",1)
      case "3G" => list:+=("3G D00020003",1)
      case "2G" => list:+=("2G D00020004 ",1)
      case _ =>list:+=("D00020005",1)
    }
    //运营商标签
    val ispname = row.getAs[String]("ispname")
    val ispid = row.getAs[Int]("ispid")
    if (StringUtils.isNoneBlank(ispname)){
      list:+=(ispname+" D0003000"+ispid,1)
    }else{
      list:+=("D00030004",1)
    }
      list
  }
}
