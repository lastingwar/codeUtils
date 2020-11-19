package com.lastingwar.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yhm
 * @create 2020-10-31 11:33
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建StreamingContext,该对象是提交SparkStreamingApp的入口,3s是批处理间隔
    val ssc = new StreamingContext(conf,Seconds(5))

    val value: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

    value.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .print()

    //启动采集器
    ssc.start()

    // 4. 默认情况下，上下文对象不能关闭
    //ssc.stop()


    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }
}


/**
 * @param host ： 主机名称
 * @param port ： 端口号
 *  Receiver[String] ：返回值类型：String
 *  StorageLevel.MEMORY_ONLY： 返回值存储方式
 */
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  // 最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {

    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  // 读数据并将数据发送给Spark
  def receive(): Unit = {

    // 创建一个Socket
    var socket: Socket = new Socket(host, port)

    // 创建一个BufferedReader用于读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

    // 读取数据
    var input: String = reader.readLine()

    //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
    while (!isStopped() && input != null) {
      store(input)
      input = reader.readLine()
    }

    // 如果循环结束，则关闭资源
    reader.close()
    socket.close()

    //重启接收任务
    restart("restart")
  }

  override def onStop(): Unit = {}
}
