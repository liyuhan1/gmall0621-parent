package com.atguigu.gmall.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Desc: 从MySQL中查询数据
  */
object MySQLUtil {
  /**
    List[
        {"id":1,"name":"zs"},
        {"id":2,"name":"ww"}
    ]
    */
  def queryList(sql:String): List[JSONObject] ={
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    //注册驱动
    Class.forName("com.mysql.jdbc.Driver")
    //创建连接
    val conn: Connection = DriverManager.getConnection(
      "jdbc:mysql://hadoop102:3306/gmall0621_rs?characterEncoding=utf-8&useSSL=false",
      "root",
      "123456"
    )
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val metaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while(rs.next()){
      val jsonObj = new JSONObject()
      for( i <- 1 to metaData.getColumnCount){
        jsonObj.put(metaData.getColumnName(i),rs.getObject(i))
      }
      rsList.append(jsonObj)
    }

    //释放资源
    rs.close()
    ps.close()
    conn.close()
    rsList.toList
  }

  def main(args: Array[String]): Unit = {
    val jsonList: List[JSONObject] = queryList("select * from offset_0621")
    println(jsonList)
  }

}
