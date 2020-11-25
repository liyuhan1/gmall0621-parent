package com.atguigu.gmall.dws

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.utils.{MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Desc: 从Kafka中读取dwd_order_info和dwd_order_detail 进行双流合并，计算实付分摊
 */
object OrderWideApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "dwd_order_info"
    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "dwd_order_detail"

    //从redis中读取偏移量   （启动执行一次）
    val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, orderDetailGroupId)

    //根据订单偏移量，从Kafka中获取订单数据
    var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
    } else {
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }


    //根据订单明细偏移量，从Kafka中获取订单明细数据
    var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
    } else {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }


    //从流中获得本批次的 订单偏移量结束点（每批次执行一次）
    var orderInfoOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //从流中获得本批次的 订单明细偏移量结束点（每批次执行一次）
    var orderDetailOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //提取订单数据
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        orderInfo
      }
    }

    //提取明细数据
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map {
      record => {
        val jsonString: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
      }
    }

    //    orderInfoDstream.print(1000)
    //    orderDetailDstream.print(1000)

    //双流join 问题:订单详情不一定在同一个采集周期中被采集
    /*val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)

    joinedDStream.print(1000)*/

    //滑窗  + 去重实现双流join
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(30), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(30), Seconds(5))

    //将订单和订单明细DS结构进行转换，转换为kv
    val orderInfoWindowWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWindowWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    //双流合并
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowWithKeyDStream.join(orderDetailWindowWithKeyDStream)

    //去重    Redis   type：set   key:order_join:orderId  value ? orderDetailId  expire 60*10
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        val jedis: Jedis = MyRedisUtil.getJedis
        //定义一个新的结合，来存放合并之后的数据
        val orderWideList: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          //拼接redis的key
          var key = "order_join:" + orderId
          val isNotExist: java.lang.Long = jedis.sadd(key, orderDetail.id.toString)
          if (jedis.ttl(key) < 0) {
            jedis.expire(key, 600)
          }
          if (isNotExist == 1L) {
            //说明不重复，以前没有进行过订单和该明细的合并，封装一个OrderWide对象
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedis.close()
        orderWideList.toIterator
      }
    }
    //orderWideDStream.print(1000)

    //实付金额分摊实现
    //Reids type:String     key:order_origin_sum:order_id   value:累计金额  失效时间:600
    val splitOrderWideDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        //获取redis连接
        val jedis: Jedis = MyRedisUtil.getJedis
        //将迭代器转换为集合
        val orderWideList: List[OrderWide] = orderWideItr.toList
        for (orderWide <- orderWideList) {
          //从Redis中获取处理过的明细单价*数量合计
          var originSumKey = "order_origin_sum:" + orderWide.order_id
          var originSum: Double = 0D
          val originSumStr: String = jedis.get(originSumKey)
          //注意：从Redis中取出的数据都要进行非空判断
          if (originSumStr != null && originSumStr.size > 0) {
            originSum = originSumStr.toDouble
          }
          //从Redis中获取处理过的明细分摊金额的合计
          var splitSumKey = "order_split_sum:" + orderWide.order_id
          var splitSum = 0D
          val splitSumStr: String = jedis.get(splitSumKey)
          if (splitSumStr != null && splitSumStr.size > 0) {
            splitSum = splitSumStr.toDouble
          }

          //当前明细的单价*数量
          var detailAmount = orderWide.sku_price * orderWide.sku_num

          //判断是否为最后一条
          if (detailAmount == orderWide.original_total_amount - originSum) {
            //是最后一条 减法
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - splitSum) * 100D) / 100D
          } else {
            //不是最后一条  乘除法
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100D) / 100D
            //将计算之后的累加金额保存到Redis中
            var newOriginSum = originSum + detailAmount
            jedis.setex(originSumKey, 600, newOriginSum.toString)

            var newSplitSum = splitSum + orderWide.final_detail_amount
            jedis.setex(splitSumKey, 600, newSplitSum.toString)
          }

        }

        //关闭连接
        jedis.close()
        orderWideList.toIterator
      }
    }
    splitOrderWideDStream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
