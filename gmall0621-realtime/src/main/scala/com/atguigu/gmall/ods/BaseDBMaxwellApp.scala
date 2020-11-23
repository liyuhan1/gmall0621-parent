package com.atguigu.gmall.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic = "gmall0621_db_m"
    val groupId = "base_db_maxwell_group"

    //获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //根据偏移量到Kafka中读取数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //获取当前批次读取的kakfa主题中分区的偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //对采集到的数据进行结构的转换   ConsumerRecord =>JSONObject
    val jsonDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取json格式字符串（消息）
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }
    //分流处理
    jsonDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            //获取操作类型
            val opType: String = jsonObj.getString("type")
            //获取表名
            val tableName: String = jsonObj.getString("table")
            //获取数据
            val dataObj: JSONObject = jsonObj.getJSONObject("data")
            if (dataObj != null && !dataObj.isEmpty) {
              if (
                ("order_info".equals(tableName) && "insert".equals(opType))
                  || (tableName.equals("order_detail") && "insert".equals(opType))
                  || tableName.equals("base_province")
                  || tableName.equals("user_info")
                  || tableName.equals("sku_info")
                  || tableName.equals("base_trademark")
                  || tableName.equals("base_category3")
                  || tableName.equals("spu_info")
              ) {
                //拼接目标主题名
                var sendTopic = "ods_" + tableName
                //发送到kafka
                MyKafkaSink.send(sendTopic, dataObj.toString())
              }
            }
          }
        }
        //保存偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
