package com.lastingwar.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2020-09-28 20:32
 */
object SparkSQL02_RDDAndDataFrame {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2. 创建SparkContext,该对象是提交SparkApp的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/user.txt")

    val userRDD: RDD[(String, Int)] = lineRDD.map {
      x =>
        val fields: Array[String] = x.split(",")
        (fields(0), fields(1).trim.toInt)
    }

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    // 5.2 RDD转换为 DataFrame(手动转)
    userRDD.toDF("name","age").show

    // 5.3 RDD转换为 DataFrame(通过样例类反射转)
    val userDataFrame: DataFrame = userRDD.map {
      x => User(x._1, x._2)
    }.toDF()

    userDataFrame.show()

    // 5.4 DataFrame 转换为 RDD
    val userRDD1: RDD[Row] = userDataFrame.rdd

    userRDD1.collect().foreach(println)
    // 4. 关闭sc
    sc.stop()
  }
  case class User(name:String,age:Long)
}



