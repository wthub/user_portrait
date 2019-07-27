package com.app

import com.Parquets.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object KeyTag extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //停用关键字广播Arry
    val stopKey = args(1).asInstanceOf[Broadcast[Array[String]]]
    val row = args(0).asInstanceOf[Row]
    //关键字字符串
    val keywords = row.getAs[String]("keywords")
    //切割关键字字符串
    val keys = keywords.split("\\|")
    if(keys.size>0){
      for (k<-keys){
        //判断是否符合关键字标签要求
        if(!stopKey.value.contains(k) && k.length>=3 && k.length<=8){
          list:+=("K"+k,1)
        }
      }
    }
    list
  }
}
