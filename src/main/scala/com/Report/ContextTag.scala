package com.Report

import com.Constants.tagUtils
import com.app._
import com.config.ConfigManager
import com.parqduetUtils.SparkSsUtils
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName, mapreduce}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object ContextTag {
  def main(args: Array[String]): Unit = {
    val ss = SparkSsUtils.getSarksess()

    //加载本地配置
    val tableName = ConfigManager.getProper("hbase.table.Name")
    val zook = ConfigManager.getProper("hbase.zookeeper.host")
    val configuration = ss.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", zook)
    //创建conn连接
    val hbConn = ConnectionFactory.createConnection(configuration)
    val admin = hbConn.getAdmin
    //判断表是否存在不存在则建
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      //获取建表对象
      val hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      //获取创建列簇对象
      val hColumnDescriptor = new HColumnDescriptor("tags")
      //将列簇加载到表中
      hTableDescriptor.addFamily(hColumnDescriptor)
      //加载表
      admin.createTable(hTableDescriptor)
      admin.close()
      hbConn.close()
    }
    //加载hbase的相关属性配置
    val jobconf = new JobConf(configuration)
    //指定Key输出类型
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出表
    jobconf.set(mapreduce.TableOutputFormat.OUTPUT_TABLE, tableName)


    //读取app字典文件
    val map: Map[String, String] = ss.sparkContext.textFile("file:///D:\\A资料\\项目\\项目（二）03\\Spark用户画像分析\\app_dict.txt").filter(_.split(",", -1).length >= 5)
      .map(x => (x.split(",", -1)(4), x.split(",", -1)(4)))
      .collect().toMap
    //读取停用关键字文件
    val array = ss.sparkContext.textFile("file:///D:\\A资料\\项目\\项目（二）03\\Spark用户画像分析\\stopkeys.txt").collect()
    //广播app应用map
    val bro: Broadcast[Map[String, String]] = ss.sparkContext.broadcast(map)
    //广播停用关键字Array
    val noKeys = ss.sparkContext.broadcast(array)
    //读取数据文件
    val df = ss.read.parquet("/in/part-00000-dadd3ad9-40d9-4fa5-8314-c84105e4dc93-c000.snappy.parquet")
    import ss.implicits._
    val allRDD = df.filter(tagUtils.userIDone)
      .map(row => {
        //获得一条数据所有非空ID
        val userAllID = tagUtils.getAlluserID(row)
        (userAllID, row)
      }).rdd

    //构建点集合
    val verties = allRDD.flatMap(tp => {
      val row = tp._2
      //广告标签
      val adList = AdTags.makeTags(row)
      //app应用标签
      val appList = AppTag.makeTags(row, bro)
      //地域标签：省份、城市
      val areaList = AreaTag.makeTags(row)
      //设备标签：系统、网络类型、运营商
      val devList = DevTag.makeTags(row)
      //关键字标签
      val keyList = KeyTag.makeTags(row, noKeys)
      //商圈标签
      val busines = BusinesTag.makeTags(row)
      //合并标签
      val allTags = adList ++ appList ++ areaList ++ devList ++ keyList ++ busines
      //进行点集合构建
      val VD = tp._1.map((_, 0)) ++ allTags
      tp._1.map(uid => {
        //取头部信息与每个循环的
        if (tp._1.head.equals(uid)) {
          (uid.hashCode.toLong.toLong, VD)
        } else {
          //如果不相同就返回空集合
          (uid.hashCode.toLong, List.empty)
        }
      })
    })
    //构建边集合
    val edge: RDD[Edge[Int]] = allRDD.flatMap(tp => {
      tp._1.map(uid => Edge(tp._1.head.hashCode.toLong, uid.hashCode.toLong, 0))
    })
    //构建图
    val gra = Graph(verties,edge)

    //取出顶点
    val vertices: VertexRDD[VertexId] = gra.connectedComponents().vertices

    //将顶点和我们数据进行join
    vertices.join(verties).map{
      case (uId,(comId,tagsAndUserID))=>{
        (comId,tagsAndUserID)
      }
    }
      .reduceByKey(
        (list1,list2)=>(list1++list2)
      .groupBy(_._1).mapValues(_.map(_._2).sum).toList)
    //存入hbase数据库



  }
}