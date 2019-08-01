package com.Constants

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/*
   标签工具
 */
object tagUtils {

  //过滤条件
  val userIDone=
    """
      |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
      |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
      |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
    """.stripMargin
  //获取不为空的用户ID
  def getOneUserID(row:Row):String={
    row match {
      case x if StringUtils.isNoneBlank(x.getAs[String]("imei")) => "IM: "+x.getAs[String]("imei")
      case x if StringUtils.isNoneBlank(x.getAs[String]("mac")) => "MC: "+x.getAs[String]("mac")
      case x if StringUtils.isNoneBlank(x.getAs[String]("idfa")) => "ID: "+x.getAs[String]("idfa")
      case x if StringUtils.isNoneBlank(x.getAs[String]("openudid")) => "OD: "+x.getAs[String]("openudid")
      case x if StringUtils.isNoneBlank(x.getAs[String]("androidid")) => "AD: "+x.getAs[String]("androidid")
      case x if StringUtils.isNoneBlank(x.getAs[String]("imeimd5")) => "IMMD5: "+x.getAs[String]("imeimd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("macmd5")) => "MCMD5: "+x.getAs[String]("macmd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("idfamd5")) => "IDMD5: "+x.getAs[String]("idfamd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("openudidmd5")) => "ODMD5: "+x.getAs[String]("openudidmd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("androididmd5")) => "ADMD5: "+x.getAs[String]("androididmd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("imeisha1")) => "IMSI: "+x.getAs[String]("imeisha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("macsha1")) => "MCSI: "+x.getAs[String]("macsha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("idfasha1")) => "IDSI: "+x.getAs[String]("idfasha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("openudidsha1")) => "ODSI: "+x.getAs[String]("openudidsha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("androididsha1")) => "ADSI: "+x.getAs[String]("androididsha1")

    }
  }

  def getAlluserID(v:Row):List[String]={
    var list = List[String]()
    if(StringUtils.isNoneBlank(v.getAs[String]("imei"))) list:+= "IM: "+v.getAs[String]("imei")
    if(StringUtils.isNoneBlank(v.getAs[String]("mac")))  list:+= "MC: "+v.getAs[String]("mac")
    if(StringUtils.isNoneBlank(v.getAs[String]("idfa")))  list:+= "ID: "+v.getAs[String]("idfa")
    if(StringUtils.isNoneBlank(v.getAs[String]("openudid"))) list:+=  "OD: "+v.getAs[String]("openudid")
    if(StringUtils.isNoneBlank(v.getAs[String]("androidid"))) list:+=  "AD: "+v.getAs[String]("androidid")
    if(StringUtils.isNoneBlank(v.getAs[String]("imeimd5")))  list:+= "IMMD5: "+v.getAs[String]("imeimd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("macmd5")))  list:+= "MCMD5: "+v.getAs[String]("macmd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("idfamd5")))  list:+= "IDMD5: "+v.getAs[String]("idfamd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("openudidmd5")))  list:+= "ODMD5: "+v.getAs[String]("openudidmd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("androididmd5"))) list:+=  "ADMD5: "+v.getAs[String]("androididmd5")
    if(StringUtils.isNoneBlank(v.getAs[String]("imeisha1")))  list:+= "IMS1: "+v.getAs[String]("imeisha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("macsha1")))  list:+= "MCS1: "+v.getAs[String]("macsha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("idfasha1")))  list:+= "IDS1: "+v.getAs[String]("idfasha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("openudidsha1")))  list:+= "ODS1: "+v.getAs[String]("openudidsha1")
    if(StringUtils.isNoneBlank(v.getAs[String]("androididsha1"))) list:+=  "ADS1: "+v.getAs[String]("androididsha1")
    list
  }

}
