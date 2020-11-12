package com.lastingwar.app

import com.alibaba.fastjson.JSON
import com.lastingwar.bean.UserInfo
import com.lastingwar.gmall.constant.GmallConstants
import com.lastingwar.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author yhm
 * @create 2020-11-11 18:05
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,5s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(5))

    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_USER_INFO, ssc)

    // 转换并写入redis
    userInfoKafkaDStream.map(_.value()).foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        // 获取redis的连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach(userInfoJson =>{
          // 取得userInfo的id
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          val userInfoRedisKey = s"UserInfo:${userInfo.id}"
          //写入redis
          jedisClient.set(userInfoRedisKey,userInfoJson)

        })

        // 关闭连接
        jedisClient.close()
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
