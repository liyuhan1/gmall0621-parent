package com.atguigu.gmall.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManagerUtil {

  /**
   * 从Redis中读取偏移量
   * Reids格式：type=>Hash
   * key=>offset:topic:groupId
   * field=>partitionId
   * value=>偏移量值 Map[TopicPartition,Long]
   * expire 不需要指定
   */
  def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
    //获取Redis客户端
    val jedis: Jedis = MyRedisUtil.getJedis
    //拼接Reids中存储偏移量的key
    //val offsetKey: String = "offset:" + topicName + ":" + groupId
    val offsetKey: String = s"offset:$topicName:$groupId"
    //根据key从Reids中获取数据
    val offsetMap: java.util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()
    //将Java的Map转换为Scala的Map，方便后续操作
    import scala.collection.JavaConverters._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partitionId, offset) => {
        println("读取分区偏移量：" + partitionId + ":" + offset)
        //将Redis中保存的分区对应的偏移量进行封装
        (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
      }
    }.toMap
    kafkaOffsetMap
  }

  /**
   * 向Redis中保存偏移量
   * Reids格式：type=>Hash
   * key=>offset:topic:groupId
   * field=>partitionId
   * value=>偏移量值 Map[TopicPartition,Long]
   * expire 不需要指定
   */
  def saveOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {

    //定义Java的map集合，用于向Reids中保存数据
    val offsetMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
    //对封装的偏移量数组offsetRanges进行遍历
    for (offset <- offsetRanges) {
      //获取分区
      val partition: Int = offset.partition
      //获取结束点
      val untilOffset: Long = offset.untilOffset
      //封装到Map集合中
      offsetMap.put(partition.toString, untilOffset.toString)
      //打印测试
      println("保存分区:" + partition + ":" + offset.fromOffset + "--->" + offset.untilOffset)
    }

    //拼接Reids中存储偏移量的key
    //val offsetKey: String = "offset:" + topicName + ":" + groupId
    val offsetKey: String = s"offset:$topicName:$groupId"

    //如果需要保存的偏移量不为空 执行保存操作
    if (offsetMap != null && offsetMap.size() > 0) {
      //获取Redis客户端
      val jedis: Jedis = MyRedisUtil.getJedis

      //保存到Redis中
      jedis.hmset(offsetKey, offsetMap)
      //关闭客户端
      jedis.close()
    }
  }

}
