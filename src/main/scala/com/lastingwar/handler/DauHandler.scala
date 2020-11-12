package com.lastingwar.handler

import java.util

import com.lastingwar.bean.StartUpLog
import com.lastingwar.utils.redis.RedisGetClientUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author yhm
 * @create 2020-11-05 14:14
 */
object DauHandler {
  /**
   * 同批次去重
   *
   * @param redisDstream
   */
  def filterByMid(redisDstream: DStream[StartUpLog]): DStream[StartUpLog] = {
    // 变换结构,group去重
    val dtMidToLog: DStream[((String, String), Iterable[StartUpLog])] = redisDstream.map(ds => ((ds.logDate, ds.mid), ds)).groupByKey()

    val MidDstream: DStream[StartUpLog] = dtMidToLog.map {
      case ((dt, mid), log) => log.toList.take(1).head
    }
    MidDstream
  }

  /**
   * 不同批次去重
   *
   * @param startUpLogDstream
   * @return
   */
  def filterByRedis(startUpLogDstream: DStream[StartUpLog]): DStream[StartUpLog] = {
    //方案二：分区内获取连接
    val redisDStream: DStream[StartUpLog] = startUpLogDstream.transform(rdd => {
      rdd.mapPartitions(iter => {
        // 获取连接
        val jedisClient: Jedis = RedisGetClientUtil.getJedisClient
        // 过滤
        val upLogs: Iterator[StartUpLog] = iter.filter { log =>
          val mid: util.Set[String] = jedisClient.smembers(s"DAU:${log.logDate}")
          !mid.contains(log.mid)
        }
        jedisClient.close()
        // 返回过滤后的迭代器
        upLogs
      })
    })
    redisDStream
  }


  /**
   * 将mid写入到redis中,供后续去重
   *
   * @param startUpLogDstream
   */
  def saveMidToRedis(startUpLogDstream: DStream[StartUpLog]): Unit = {
    startUpLogDstream.foreachRDD(rdd => {
      rdd.foreachPartition { iter => {
        // a.获取连接
        val jedisClint: Jedis = RedisGetClientUtil.getJedisClient
        iter.foreach(log => {
          // b.遍历写库
          val redisKey = s"DAU:${log.logDate}"
          jedisClint.sadd(redisKey, log.mid)
        })
        // c.归还连接
        jedisClint.close()
      }
      }
    })
  }
}
