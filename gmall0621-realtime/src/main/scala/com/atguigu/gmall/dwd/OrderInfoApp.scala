package com.atguigu.gmall.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.atguigu.gmall.utils.{MyESUtil, MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_info"
    val groupId = "order_info_group"

    //获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //根据偏移量到Kafka中消费数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前采集周期对Kafka中主题分区的消费情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //将读取到的数据进行结构的转换    ConsumerRecord===>OrderInfo
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        //获取json字符串
        val jsonStr: String = record.value()
        //根据json字符串转换为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        //获取创建时间  2020-11-21 13:51:56
        val createTime: String = orderInfo.create_time
        val createTimeArr: Array[String] = createTime.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }
    //orderInfoDStream.print(1000)
    /*
    //判断是否为首单
    // 方案1   对采集周期的数据逐条进行处理  缺点：查询过于频繁
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        //获取下单用户id
        val userId: Long = orderInfo.user_id
        var sql: String = s"select user_id,if_consumed from user_status0621 where user_id ='${userId}'"
        val consumedList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if (consumedList != null && consumedList.size > 0) {
          //曾经消费过，不是首单用户
          orderInfo.if_first_order = "0"
        } else {
          //还没有消费过，是首单用户
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }
    orderInfoWithFirstFlagDStream.print(1000)
  */
    //判断是否为首单
    //方案2   以分区为单位进行处理
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //因为迭代器只能会迭代一次，为了操作方便，我们将其转换为List集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中所有下单用户的id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //根据用户id到Phoenix中查询用户状态   坑1：查询字符串的拼接
        var sql: String = s"select user_id,if_consumed from user_status0621 where user_id in('${userIdList.mkString("','")}')"
        val userStatusJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //获取已经消费的用户id   坑2：查询出来的字段是大写
        val consumedUserIdList: List[String] = userStatusJsonList.map(_.getString("USER_ID"))

        //对订单进行遍历
        for (orderInfo <- orderInfoList) {
          //坑3  orderInfo.user_id获取是Long类型，但是consumedUserIdList是String类型，需要进行类型转换
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            //消费过，非首单
            orderInfo.if_first_order = "0"
          } else {
            //首单
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }

    //===============4.同批次状态修正=================
    //因为要使用groupByKey对用户进行分组，所以先对DStream中的数据结构进行转换
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map {
      orderInfo => {
        (orderInfo.user_id, orderInfo)
      }
    }
    //按照用户id对当前采集周期数据进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithKeyDStream.groupByKey()
    //对分组后的用户订单进行判断
    val orderInfoRealWithFirstFlagDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (userId, orderInfoItr) => {
        //如果同一批次有用户的订单数量大于1了
        if (orderInfoItr.size > 1) {
          //对用户订单按照时间进行排序
          val sortedList: List[OrderInfo] = orderInfoItr.toList.sortWith(
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          )
          //获取排序后集合的第一个元素
          val orderInfoFirst: OrderInfo = sortedList(0)
          //判断是否为首单
          if (orderInfoFirst.if_first_order == "1") {
            //将除了首单的其它订单设置为非首单
            for (i <- 1 until sortedList.size) {
              sortedList(i).if_first_order = "0"
            }
          }
          sortedList
        } else {
          orderInfoItr.toList
        }
      }
    }


    //===============5.和省份进行维度关联================
    /*
    //5.1 方案1 以分区为单位和省份进行关联
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealWithFirstFlagDStream.mapPartitions {
      orderInfoItr => {
        //将当前分区中的订单转换为List
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中订单下单的省份
        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
        //根据省份id，到Phoenix的省份维度表中将所有省份信息查询出来
        var sql: String
        = s"select id,name,area_code,iso_code from gmall0621_province_info where id in('${provinceIdList.mkString("','")}')"
        val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //对json集合进行结构的转换
        val provinceMap: Map[String, ProvinceInfo] = provinceJsonObjList.map {
          provinceJsonObj => {
            //将json对象转换为对应的样例类对象
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        for (orderInfo <- orderInfoList) {
          val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id.toString, null)
          if (provinceInfo != null) {
            orderInfo.province_name = provinceInfo.name
            orderInfo.province_area_code = provinceInfo.area_code
            orderInfo.province_iso_code = provinceInfo.iso_code
          }
        }
        orderInfoList.toIterator
      }
    }
    orderInfoWithProvinceDStream.print(1000)
    */
    //5.2 方案2 以采集周期为单位和省份进行关联
    // 好处：如果分区比较多，可以减少查询次数    弊端：如果维度数据比较大，对Driver压力比较大
    //transform和foreachRDD这两个算子比较特殊，其中rdd算子外的代码在Driver执行，每个采集周期执行一次
    //相当于将对RDD的封装暴露给了调用者
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealWithFirstFlagDStream.transform {
      rdd => {
        //获取全部省份数据
        var sql: String = "select id,name,area_code,iso_code from gmall0621_province_info"
        val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //转换结构   jsonList==>ProvinceMap
        val provinceMap: Map[String, ProvinceInfo] = provinceJsonObjList.map {
          provinceJsonObj => {
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap
        //使用广播变量对Map进行封装
        val provinceMapBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)

        rdd.map {
          orderInfo => {
            val provinceInfo: ProvinceInfo = provinceMapBC.value.getOrElse(orderInfo.province_id.toString, null)
            if (provinceInfo != null) {
              orderInfo.province_name = provinceInfo.name
              orderInfo.province_area_code = provinceInfo.area_code
              orderInfo.province_iso_code = provinceInfo.iso_code
            }
            orderInfo
          }
        }
      }
    }

    //orderInfoWithProvinceDStream.print(1000)


    //===============6.订单和用户维度进行关联================
    //以分区为单位进行处理
    val orderInfoWithUserInfoDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区订单对应的所有下单用户id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //拼接查询sql
        var sql: String = s"select id,user_level,birthday,gender,age_group,gender_name from gmall0621_user_info where id in('${userIdList.mkString("','")}')"
        //执行查询操作，到Phoenix中的用户表中获取数据
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //将List集合转换为map,并且将json对象转换为用户样例类对象
        val userMap: Map[String, UserInfo] = userJsonObjList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap

        for (orderInfo <- orderInfoList) {
          val userInfo: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfo != null) {
            orderInfo.user_gender = userInfo.gender_name
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
            orderInfo.user_age_group = userInfo.age_group
          }
        }
        orderInfoList.toIterator
      }
    }
    //orderInfoWithUserInfoDStream.print(1000)


    //===============3.保存=================
    //导入类下成员
    import org.apache.phoenix.spark._
    orderInfoWithUserInfoDStream.foreachRDD {
      rdd => {
        rdd.cache()
        //从所有订单中，将首单的订单过滤出来
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")
        //获取当前订单用户并更新到Hbase，注意：saveToPhoenix在更新的时候，要求rdd中的属性和插入hbase表中的列必须保持一致，所以转换一下
        val firstOrderUserRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }
        firstOrderUserRDD.saveToPhoenix(
          "USER_STATUS0621",
          Seq("USER_ID", "IF_CONSUMED"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        //保存
        rdd.foreachPartition {
          orderInfoItr => {
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString, orderInfo))
            //保存到es
            /*val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "gmall0621_order_info_" + dateStr)*/

            //3.3将订单数据保存到Kafka的dwd_order_info
            for ((_, orderInfo) <- orderInfoList) {
              MyKafkaSink.send("dwd_order_info", JSON.toJSONString(orderInfo, new SerializeConfig(true)))
            }
          }
        }

        //保存偏移量到Redis
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
