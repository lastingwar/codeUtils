package com.lastingwar.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
 * @author yhm
 * @create 2020-11-01 11:31
 */
object SparkStreaming09_reduceByKeyAndWindow_reduce {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(3))


    ssc.checkpoint("./ck1")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordToOne: DStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))

    val wordToSumDStream: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => (x + y),
      (x: Int, y: Int) => (x - y),
      Seconds(12),
      Seconds(6),
      new HashPartitioner(2),
      (x:(String,Int)) => x._2 >0
    )

    wordToSumDStream.print()

    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()


    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
