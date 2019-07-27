package com.app

import ch.hsr.geohash.GeoHash
import com.Parquets.Tags
import com.parqduetUtils.{GdHandler, JedisConnectionPool}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object BusinesTag extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long").toDouble
    val lat = row.getAs[String]("lat").toDouble
    if(long>=73 &&
      long<=135 &&
      lat>=3 &&
      lat<= 54) {
      val businesss = getBusiness(long, lat)
      if (StringUtils.isNoneBlank(businesss)) {
        for (b <- businesss.split(",")) {
          list :+= (b, 1)
        }
      }
    }
    list
  }


  def insertBusin(geoHash: GeoHash, business: String) = {
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geoHash.toString,business)
    jedis.close()
  }

  def getBusiness(long:Double, lat:Double):String={
    val geoHash = GeoHash.withCharacterPrecision(lat,long,8)
    var business = redisTbuess(geoHash)
    if(business==null || business.length==0){
      business = GdHandler.getBusiness(long,lat)
      if(StringUtils.isNoneBlank(business) || business.length>0){
        insertBusin(geoHash,business)
      }
    }

    business
  }
  def redisTbuess(geoHash: GeoHash):String = {
   val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geoHash.toString)
    jedis.close()
    business
  }
}
