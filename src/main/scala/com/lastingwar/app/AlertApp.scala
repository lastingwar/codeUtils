package com.lastingwar.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.lastingwar.bean.{CouponAlertInfo, EventLog}
import com.lastingwar.utils.es.MyEsWriteUtil
import com.lastingwar.utils.kafka.MyKafkaGetStreamUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * 预警业务类 从kafka引入,使用sparkStream处理写入es
 *
 * @author yhm
 * @create 2020-11-10 11:15
 */
object AlertApp {

  final val KAFKA_TOPIC_EVENT = "GMALL_EVENT"
  final val ES_ALERT_INDEX_PRE = "gmall_coupon_alert"

  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,5s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaGetStreamUtil.getKafkaStream(KAFKA_TOPIC_EVENT, ssc)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    // 处理kafkaDStream
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(record => {
      // 转换样例类
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      // 处理时间
      val dateTime: Array[String] = sdf.format(eventLog.ts).split(" ")
      eventLog.logDate = dateTime(0)
      eventLog.logHour = dateTime(1)
      // 根据设备分组
      (eventLog.mid, eventLog)
    })

    // 开窗 5min
    val midToLogWindowsDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    // 根据设备分组
    // 同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。达到以上要求则产生一条预警日志。并且同一设备，每分钟只记录一次预警
    // 返回类型 (是否预警,(设备id,uid集合,涉及商品集合,evid集合,当前时间戳))
    val couponInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogWindowsDStream.groupByKey().map {
      case (mid, logs) =>
        // 创建List用于存放反生过的所有行为evid
        val eventArr = new util.ArrayList[String]()
        // 涉及优惠券的商品id
        val itemIdSet = new util.HashSet[String]()
        // 存放不重复的uid
        val uidSet = new util.HashSet[String]()
        // 判断是否为点击商品
        var clickFlat = false

        // 判断是否有点击,并添加不同的uid
        breakable {
          logs.foreach(log => {

            val evid: String = log.evid
            // 存放行为evid
            eventArr.add(evid)

            if ("clickItem".equals(evid)) {
              clickFlat = true
              break
            }
            else if ("coupon".equals(evid)) {
              itemIdSet.add(log.itemid)
              uidSet.add(log.uid)
            }
          })
        }
        (!clickFlat && uidSet.size() >= 3, CouponAlertInfo(mid, uidSet, itemIdSet, eventArr, System.currentTimeMillis()))
    }

    // 预警日志
    val alertInfoDStream: DStream[CouponAlertInfo] = couponInfoDStream.filter(_._1).map(_._2)

    alertInfoDStream.print()
    println("111111")

    // 写入es
    alertInfoDStream.foreachRDD(rdd =>{
      rdd.foreachPartition( iter =>{
        // 创建索引名
        val todayStr: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        val indexName = s"${ES_ALERT_INDEX_PRE}-$todayStr"

        // 处理数据 补充docId
        val docList: List[(String, CouponAlertInfo)] = iter.toList.map(alterInfo => {
          val min: Long = alterInfo.ts / 1000 / 60
          (s"${alterInfo.mid}-$min", alterInfo)
        })


        // 执行写入操作
        MyEsWriteUtil.insertBulk(indexName,docList)
//        println(indexName)
      })
    })



    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()


    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()

  }
}
