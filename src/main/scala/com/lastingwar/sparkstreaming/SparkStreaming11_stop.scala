package com.lastingwar.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author yhm
 * @create 2020-11-02 11:45
 * 监控器优雅关闭sparkStream
 */
object SparkStreaming11_stop {

  val update = (values:Seq[Int],status:Option[Int]) => {

    //当前批次内容的计算
    val sum: Int = values.sum

    //取出状态信息中上一次状态
    val lastStatu: Int = status.getOrElse(0)

    Some(sum + lastStatu)
  }

  def createSSC():StreamingContext = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("sparkstreaming")

    // 设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 设置检查点
    ssc.checkpoint("./ck2")

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    line.flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey(update)
      .print()

    ssc
  }


  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck2", () => createSSC())

    new Thread(new MonitorStop(ssc)).start()

    ssc.start()
    ssc.awaitTermination()
  }
}

class MonitorStop(ssc:StreamingContext) extends Runnable{
  override def run(): Unit = {
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"), new Configuration(), "atguigu")

    while (true){
      Thread.sleep(5000)

      val result: Boolean = fs.exists(new Path("hdfs://hadoop102:8020/stopSpark"))

      if(result){
        val state: StreamingContextState = ssc.getState()

        if (state == StreamingContextState.ACTIVE){
          ssc.stop(stopSparkContext = true,stopGracefully = true)
          System.exit(0)
        }
      }

    }
  }
}
