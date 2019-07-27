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
      case x if StringUtils.isNoneBlank(x.getAs[String]("mac")) => "MAC: "+x.getAs[String]("mac")
      case x if StringUtils.isNoneBlank(x.getAs[String]("idfa")) => "ID: "+x.getAs[String]("idfa")
      case x if StringUtils.isNoneBlank(x.getAs[String]("openudid")) => "IM: "+x.getAs[String]("openudid")
      case x if StringUtils.isNoneBlank(x.getAs[String]("androidid")) => "AD: "+x.getAs[String]("androidid")
      case x if StringUtils.isNoneBlank(x.getAs[String]("imeimd5")) => "IM: "+x.getAs[String]("imeimd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("macmd5")) => "MA: "+x.getAs[String]("macmd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("idfamd5")) => "ID: "+x.getAs[String]("idfamd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("openudidmd5")) => "ID: "+x.getAs[String]("openudidmd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("androididmd5")) => "AD: "+x.getAs[String]("androididmd5")
      case x if StringUtils.isNoneBlank(x.getAs[String]("imeisha1")) => "IM: "+x.getAs[String]("imeisha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("macsha1")) => "MA: "+x.getAs[String]("macsha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("idfasha1")) => "ID: "+x.getAs[String]("idfasha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("openudidsha1")) => "IM: "+x.getAs[String]("openudidsha1")
      case x if StringUtils.isNoneBlank(x.getAs[String]("androididsha1")) => "AD: "+x.getAs[String]("androididsha1")

    }
  }

}
