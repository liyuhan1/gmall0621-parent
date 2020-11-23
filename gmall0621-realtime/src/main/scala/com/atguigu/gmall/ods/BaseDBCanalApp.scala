package com.atguigu.gmall.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/11/21
  * Desc: 基于canal同步mysql数据，从Kafka中将数据读取出来，并且
  *   根据表名，将数据进行分流，发送到不同的kafka主题中去
  */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    val topic = "gmall0621_db_c"
    val groupId = "base_db_canal_group"

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    //根据偏移量消费kafka中的数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0){
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    //获取当前批次对Kafka主题中分区消费的偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDSrtream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //对数据进行结构的转换      ConsumerRecord===>JSONObject
    val jsonObjDStream: DStream[JSONObject] = offsetDSrtream.map {
      record => {
        //获取json格式字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }
    //jsonObjDStream.print(1000)
    //根据表名将数据发送到Kafka不同的topic中
    jsonObjDStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          jsonObj=>{
            //获取类型
            val opType: String = jsonObj.getString("type")
            if("INSERT".equals(opType)){
              //获取表名
              val tableName: String = jsonObj.getString("table")
              //获取数据
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              //拼接要保存的主题名
              var sendTopic = "ods_"+tableName
              //将json数组转换为Scala数组
              import scala.collection.JavaConverters._
              for (data <- dataArr.asScala) {
                val msg: String = data.toString
                MyKafkaSink.send(sendTopic,msg)
              }
            }
          }
        }
        //保存偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
