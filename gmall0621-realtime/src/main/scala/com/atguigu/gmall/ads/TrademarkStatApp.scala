package com.atguigu.gmall.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.OrderWide
import com.atguigu.gmall.utils.{MyKafkaUtil, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
 * Desc: 从Kafka的dws_order_wide中读取数据，对品牌交易额进行统计
 */
object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("TrademarkStatApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "dws_order_wide";
    val groupId = "ads_trademark_stat_group"

    //获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic, groupId)

    //根据偏移量到Kafka读取数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前批次对主题分区的偏移量消费情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //进行结构的转换
    val orderWideDSteam: DStream[OrderWide] = offsetDStream.map {
      record => {
        val jsonObj: String = record.value()
        val orderWide: OrderWide = JSON.parseObject(jsonObj, classOf[OrderWide])
        orderWide
      }
    }

    //聚合
    val mapDStream: DStream[(String, Double)] = orderWideDSteam.map {
      orderWide => {
        val tmId: Long = orderWide.tm_id
        val tmName: String = orderWide.tm_name
        val amount: Double = orderWide.final_detail_amount
        (tmId + "_" + tmName, amount)
      }
    }
    val reduceDSteam: DStream[(String, Double)] = mapDStream.reduceByKey(_ + _)

    //reduceDSteam.print(1000)

    //将数据保存到MySQL数据库中(ADS层)
    /*
    //方案1：一条条的保存业务数据
    reduceDSteam.foreachRDD{
      rdd=>{
        //为了避免分布式事务，将所有数据收集到Driver端进行处理
        val tmAmountArr: Array[(String, Double)] = rdd.collect()
        if(tmAmountArr!=null && tmAmountArr.size >0){
          //加载配置文件
          DBs.setup()
          DB.localTx(
            implicit session => {
              val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              //保存业务数据
              for ((tmIdAndName, amount) <- tmAmountArr) {
                val statTime: String = sdf.format(new Date)
                val tmIdAndNameArr: Array[String] = tmIdAndName.split("_")
                val tmId: String = tmIdAndNameArr(0)
                val tmName: String = tmIdAndNameArr(1)
                val tmAmount: Double = Math.round(amount * 100D) / 100D
                println("业务数据写入执行")
                SQL("insert into trademark_amount_stat (stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                  .bind(statTime, tmId, tmName, tmAmount).update().apply()
              }
              throw new RuntimeException("测试异常")
              //保存偏移量信息
              // 写入偏移量
              for (offsetRange <- offsetRanges) {
                val partitionId: Int = offsetRange.partition
                val untilOffset: Long = offsetRange.untilOffset
                println("偏移量提交执行")
                SQL("replace into offset_0621  values(?,?,?,?)").bind(groupId, topic, partitionId, untilOffset).update().apply()
              }
            }
          )

        }
      }
    }
    */
    //方式2：批量插入
    reduceDSteam.foreachRDD {
      rdd => {
        // 为了避免分布式事务，把ex的数据提取到driver中;因为做了聚合，所以可以直接将Executor的数据聚合到Driver端
        val tmSumArr: Array[(String, Double)] = rdd.collect()
        if (tmSumArr != null && tmSumArr.size > 0) {
          DBs.setup()
          DB.localTx {
            implicit session => {
              // 写入计算结果数据
              val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val dateTime: String = formator.format(new Date())
              val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
              for ((tm, amount) <- tmSumArr) {
                val amountRound: Double = Math.round(amount * 100D) / 100D
                val tmArr: Array[String] = tm.split("_")
                val tmId = tmArr(0)
                val tmName = tmArr(1)
                batchParamsList.append(Seq(dateTime, tmId, tmName, amountRound))
              }
              //val params: Seq[Seq[Any]] = Seq(Seq("2020-08-01 10:10:10","101","品牌1",2000.00),
              // Seq("2020-08-01 10:10:10","102","品牌2",3000.00))
              //数据集合作为多个可变参数 的方法 的参数的时候 要加:_*
              SQL("insert into trademark_amount_stat(stat_time,trademark_id,trademark_name,amount) values(?,?,?,?)")
                .batch(batchParamsList.toSeq: _*).apply()
              //throw new RuntimeException("测试异常")

              // 写入偏移量
              for (offsetRange <- offsetRanges) {
                val partitionId: Int = offsetRange.partition
                val untilOffset: Long = offsetRange.untilOffset
                SQL("replace into offset_0621  values(?,?,?,?)")
                  .bind(groupId, topic, partitionId, untilOffset).update().apply()
              }

            }
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
