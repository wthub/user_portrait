package com.app

import com.Parquets.Tags
import org.apache.commons.lang3.StringUtils
//Type标签
object typeTag extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] ={
    var list = List[(String, Int)]()
    val str = args(0).asInstanceOf[String]
    if (StringUtils.isNoneBlank(str)){
      //切割去重
      val strings = str.split("\\;").distinct
      for (a<-strings){
        list:+=(a,1)
      }
    }
    list
  }
}
