package com.lastingwar.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yhm
 * @create 2020-11-01 13:30
 */
object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val windows: DStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
      .window(Seconds(12), Seconds(6))

    windows.foreachRDD(
      rdd => {
        // 在Driver端执行，一个窗口执行一次
        println("222222:" + Thread.currentThread().getName)

        rdd.foreachPartition(
          iter => iter.foreach(println)
        )
      }
    )

    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()


    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
