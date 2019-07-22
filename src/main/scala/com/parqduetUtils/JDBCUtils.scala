package com.parqduetUtils

import java.util.Properties

import com.config.ConfigManager

object JDBCUtils {

  def getJdbcProp():(Properties,String)={
    val prop = new Properties()
    prop.put("driver",ConfigManager.getProper("jdbc.driver"))
    prop.put("user",ConfigManager.getProper("jdbc.user"))
    prop.put("password",ConfigManager.getProper("jdbc.password"))
    val jdbcUrl = ConfigManager.getProper("jdbc.url")
    (prop,jdbcUrl)

  }

}
