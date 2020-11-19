package com.lastingwar.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yhm
 * @create 2020-10-30 20:36
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(3))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val flatmapDS: DStream[String] = socketDS.flatMap(_.split(" "))

    val reduceDS: DStream[(String, Int)] = flatmapDS.map{(_, 1)}.reduceByKey(_ + _)

    reduceDS.print()
    ssc.start()

    // 4. 关闭sc
//    ssc.stop()

    ssc.awaitTermination()
  }
}
