package com.config

import java.util.Properties

object ConfigManager {
  private val prop = new Properties()
  //加载配置文件
  try {
    val in_jdbc = ConfigManager.getClass.getClassLoader.getResourceAsStream("jdbc.properties")
    val in_dm = ConfigManager.getClass.getClassLoader.getResourceAsStream("dwd_dm.properties")
    val in_dmv = ConfigManager.getClass.getClassLoader.getResourceAsStream("dm.properties")
    prop.load(in_jdbc)
  }catch {
    case e:Exception=>e.printStackTrace()
  }
  def getProper(key:String):String={
    prop.getProperty(key)

  }
}
