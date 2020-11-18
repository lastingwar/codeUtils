package com.lastingwar.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yhm
 * @create 2020-10-31 22:10
 */
object sparkStreaming06_updateStateByKey {

  // 定义更新状态方法,参数seq为当前批次单词次数,state为以往批次单词次数
  val updateFunc = (seq: Seq[Int],state:Option[Int]) =>{
    // 当前批次数据累加
    val currentCount: Int = seq.sum

    val previousCount: Int = state.getOrElse(0)

    // 总的数据累加

    Some(currentCount + previousCount)
  }

  def createSCC():StreamingContext = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("./ck")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val stateDstream: DStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey[Int](updateFunc)

    stateDstream.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck", () => createSCC())

    ssc.start()
    ssc.awaitTermination()
  }
}
