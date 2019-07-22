package com.parqduetUtils

object StringUtils {

  def stringTint(str:String):Int={
    try{
      str.toInt
    }catch {
      case _:Exception=>0
    }
  }
  def stringTdouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _:Exception =>0.0
    }
  }
}
