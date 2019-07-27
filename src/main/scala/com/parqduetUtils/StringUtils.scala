package com.parqduetUtils

import com.alibaba.fastjson.{JSON, JSONArray}

/*
字符串方法
 */
object StringUtils {
  //字符串转Int
  def stringTint(str:String):Int={
    try{
      str.toInt
    }catch {
      case _:Exception=>0
    }
  }
  //字符串Double
  def stringTdouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _:Exception =>0.0
    }
  }
  //json串解析

  def stringTjson(str:String):JSONArray= {

    val json = JSON.parseObject(str)
    val st = json.getIntValue("status")
    if (st == 1) {
      val str = json.getJSONObject("regeocode")
      val array = str.getJSONArray("pois")
      array
    }else{
      null
    }
  }
}
