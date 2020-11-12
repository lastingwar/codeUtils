package com.lastingwar.app

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.lastingwar.bean.StartUpLog
import com.lastingwar.handler.DauHandler
import com.lastingwar.utils.kafka.MyKafkaGetStreamUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}



/**
 * 日活统计 从kafka引入,使用sparkStream处理保存至phoenix
 * @author yhm
 * @create 2020-11-05 10:58
 */
object DauApp {

  final val KAFKA_TOPIC_STARTUP = "GMALL_STARTUP"


  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(5))
    // a 消费kafka
    val startupLogDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaGetStreamUtil.getKafkaStream(KAFKA_TOPIC_STARTUP, ssc)

    //b 数据流 转换 结构变成case class 补充两个时间字段
//    startupLogDstream.map(_.value()).print()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDstream: DStream[StartUpLog] = startupLogDstream.map(_.value()).map {
      log =>
    val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
        val dtArr: Array[String] = sdf.format(new Date(startUpLog.ts)).split(" ")
        startUpLog.logDate = dtArr(0)
        startUpLog.logHour = dtArr(1)
        startUpLog
    }

    startUpLogDstream.cache()

    startUpLogDstream.count().print()

    //c 批次间去重
    val redisDstream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDstream)

    redisDstream.cache()
    redisDstream.count().print()
    //d 同批次去重
    val midDstream: DStream[StartUpLog] = DauHandler.filterByMid(redisDstream)

    midDstream.cache()
    midDstream.count().print()

    //e 保存访问的用户id

    DauHandler.saveMidToRedis(midDstream)


//    把数据写入hbase+phoenix
    midDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL2020_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()


    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
