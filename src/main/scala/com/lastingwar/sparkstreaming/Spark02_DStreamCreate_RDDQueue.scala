package com.lastingwar.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author yhm
 * @create 2020-10-30 21:03
 */
object Spark02_DStreamCreate_RDDQueue {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(4))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)


    val reducedStream: DStream[Int] = inputStream.reduce(_ + _)

    reducedStream.print()

    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()

    for (i<-1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }
    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}
