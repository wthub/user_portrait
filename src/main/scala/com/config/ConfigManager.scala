package com.config

import java.util.Properties

object ConfigManager {
  private val prop = new Properties()
  //加载配置文件
  try {
    val in_jdbc = ConfigManager.getClass.getClassLoader.getResourceAsStream("jdbc.properties")
    val in_pro_city = ConfigManager.getClass.getClassLoader.getResourceAsStream("mysql.properties")
    prop.load(in_jdbc)
    prop.load(in_pro_city)
  }catch {
    case e:Exception=>e.printStackTrace()
  }
  def getProper(key:String):String={
    prop.getProperty(key)

  }
}
