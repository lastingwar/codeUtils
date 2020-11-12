package com.lastingwar.app

import com.alibaba.fastjson.JSON
import com.lastingwar.bean.OrderInfo
import com.lastingwar.utils.kafka.MyKafkaGetStreamUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
 * 统计gmv 从kafka引入,使用sparkStream处理写入phoenix
 *
 * @author yhm
 * @create 2020-11-07 11:30
 */
object GmvApp {

  final val GMALL_ORDER_INFO = "TOPIC_ORDER_INFO"


  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaGetStreamUtil.getKafkaStream(GMALL_ORDER_INFO, ssc)

    // 数据处理
    val orderInfoDstream: DStream[OrderInfo] = kafkaDStream.map(_.value())
      .map(orderJson => {
        val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
        // 日期 格式: yyyy-MM-dd HH-mm-ss
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        // create_date取yyyy-MM-dd
        orderInfo.create_date = createTimeArr(0)
        // create_hour取HH
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        //收件人 电话 脱敏  取原电话的后4位加**
        orderInfo.consignee_tel = "*******" + orderInfo.consignee_tel.splitAt(7)._2

        orderInfo
      })

    // 保存到phoenix
    orderInfoDstream.foreachRDD(rdd =>{
      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        // 通过类模板返回类对象,之后获取name并改为大写
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })
    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()


    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
