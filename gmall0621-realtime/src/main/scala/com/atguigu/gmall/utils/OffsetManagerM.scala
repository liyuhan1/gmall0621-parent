package com.atguigu.gmall.utils

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
 * Desc: 从MySQL中读取偏移量
 */
object OffsetManagerM {

  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    var sql: String = s"select topic,group_id,partition_id,topic_offset from offset_0621 where topic='${topic}' and group_id='${groupId}'"
    val offsetJsonObjList: List[JSONObject] = MySQLUtil.queryList(sql)

    val offsetMap: Map[TopicPartition, Long] = offsetJsonObjList.map {
      offsetJsonObj => {
        //{"topic":"","group_id":"","partition_id":xx,"topic_offset":""}
        val partitionId: Int = offsetJsonObj.getIntValue("partition_id")
        val offsetVal: Long = offsetJsonObj.getLongValue("topic_offset")
        (new TopicPartition(topic, partitionId), offsetVal)
      }
    }.toMap
    offsetMap

  }

}
