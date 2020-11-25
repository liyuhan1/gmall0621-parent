package com.atguigu.gmall.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.{OrderDetail, SkuInfo}
import com.atguigu.gmall.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * Desc: 从Kafka的ods_order_detail中获取数据，和维度进行关联，最后将宽表写回Kafka
 */
object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_order_detail";
    val groupId = "order_detail_group"

    //从redis中读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //通过偏移量到Kafka中获取数据
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd => {
        //周期性在driver中执行
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    //提取数据
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map {
      record => {
        val jsonString: String = record.value()
        //订单处理  转换成更方便操作的专用样例类
        val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
      }
    }

    //orderDetailDstream.print(1000)

    //订单明细和商品维度(经过退化后)进行关联
    val orderDetaiWithSkuDStream: DStream[OrderDetail] = orderDetailDstream.mapPartitions {
      orderDetailItr => {
        val orderDetailList: List[OrderDetail] = orderDetailItr.toList
        //通过订单明细获取明细对应的商品的id
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        //拼接查询SQL
        var sql: String = s"select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from gmall0621_sku_info where id in('${skuIdList.mkString("','")}')"
        //执行查询操作
        val skuInfoJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //对查询的list集合进行结构转换   List->Map   JSONObject=>SkuInfo
        val skuInfoMap: Map[String, SkuInfo] = skuInfoJsonList.map {
          skuInfoJson => {
            val skuInfo: SkuInfo = JSON.toJavaObject(skuInfoJson, classOf[SkuInfo])
            (skuInfo.id, skuInfo)
          }
        }.toMap
        for (orderDetail <- orderDetailList) {
          val skuInfo: SkuInfo = skuInfoMap.getOrElse(orderDetail.sku_id.toString, null)
          if (skuInfo != null) {
            orderDetail.spu_id = skuInfo.spu_id.toLong
            orderDetail.spu_name = skuInfo.spu_name

            orderDetail.tm_id = skuInfo.tm_id.toLong
            orderDetail.tm_name = skuInfo.tm_name

            orderDetail.category3_id = skuInfo.category3_id.toLong
            orderDetail.category3_name = skuInfo.category3_name

          }
        }
        orderDetailList.toIterator
      }
    }

    //orderDetaiWithSkuDStream.print(1000)

    //将订单明细的数据写回到Kafka  dwd_order_detail
    orderDetaiWithSkuDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          orderDetail => {
            var topicName = "dwd_order_detail"
            //注意：在将对象转换为json字符串的时候，需要指定new SerializeConfig(true)，进行循环检查
            var msg: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
            MyKafkaSink.send(topicName, msg)
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
