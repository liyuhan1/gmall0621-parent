package com.atguigu.gmall.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.UserInfo
import com.atguigu.gmall.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Desc: 从Kafka的ODS层中读取用户维度数据，写入到Phoenix中
  */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ods_user_info";
    val groupId = "gmall_user_info_group"

    //获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)
    //根据偏移量读取kafka数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size > 0){
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      inputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    //获取当前采集周期对Kafka主题分区的消费情况  Array[OffsetRange]
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //结构的转换   ConsumerRecord==>UserInfo
    val userInfoDStream: DStream[UserInfo] = offsetDStream.map {
      record => {
        //获取Json字符串
        val jsonStr: String = record.value()
        //将json字符串转换为样例类对象
        val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])

        //把生日转成年龄
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date: Date = formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val betweenMs = curTs - date.getTime
        val age = betweenMs / 1000L / 60L / 60L / 24L / 365L
        if (age < 20) {
          userInfo.age_group = "20岁及以下"
        } else if (age > 30) {
          userInfo.age_group = "30岁以上"
        } else {
          userInfo.age_group = "21岁到30岁"
        }

        if (userInfo.gender == "M") {
          userInfo.gender_name = "男"
        } else {
          userInfo.gender_name = "女"
        }
        userInfo
      }
    }
    //保存到Phoenix中
    import org.apache.phoenix.spark._
    userInfoDStream.foreachRDD{
      rdd=>{
        rdd.saveToPhoenix(
          "GMALL0621_USER_INFO",
          Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        //保存偏移量到Redis
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
