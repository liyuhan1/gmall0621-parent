package com.atguigu.gmall.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.DauInfo
import com.atguigu.gmall.utils.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic = "gmall_start_0621"
    val groupId = "dau_app_group"

    //从Kafka的gmall_start_0621主题中读取数据
    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    //ConsumerRecord====>JsonObj
    val jsonObjDStream: DStream[JSONObject] = inputStream.map {
      record => {
        //获取json格式字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        //获取时间戳
        val ts: java.lang.Long = jsonObj.getLong("ts")
        //根据时间戳  转换为日期yyyy-MM-dd HH
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dayHour: String = sdf.format(new Date(ts))
        //对日期字符串进行切分，得到天和小时
        val dayHourArr: Array[String] = dayHour.split(" ")
        val dt: String = dayHourArr(0)
        val hr: String = dayHourArr(1)
        jsonObj.put("dt", dt)
        jsonObj.put("hr", hr)
        jsonObj
      }
    }

    //使用Redis过滤掉非首次登录设备 以分区为单位进行处理
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        //获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedis
        //定义一个新的集合，用来存放首次登录的设备日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        //对分区数据进行处理
        for (jsonObj <- jsonObjItr) {
          //获取当前启动日志日期
          val dt: String = jsonObj.getString("dt")
          //获取当前启动日志中的设备id
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          //拼接操作Redis的key
          val dauKey = "dau:" + dt
          //判断当前设备是否登录过
          val isNotExists: java.lang.Long = jedis.sadd(dauKey, mid)
          //设置过期时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isNotExists == 1L) {
            //没有登录过，首次登录
            filteredList.append(jsonObj)
          }
        }
        jedis.close()
        filteredList.toIterator
      }
    }

    //将首次登录设备日志保存到ES中
    filteredDStream.foreachRDD {
      rdd => {
        //以分区为单位对RDD中的数据进行处理
        rdd.foreachPartition {
          jsonObjItr => {
            val dauList: List[DauInfo] = jsonObjItr.map {
              jsonObj => {
                //将json对象转换为样例类对象
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                DauInfo(
                  commonObj.getString("mid"),
                  commonObj.getString("uid"),
                  commonObj.getString("ar"),
                  commonObj.getString("ch"),
                  commonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )
              }
            }.toList
            //将当前分区的数据批量的保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauList, "gmall0621_dau_info_" + dt)

          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
