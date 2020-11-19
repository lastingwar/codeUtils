package com.lastingwar.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author yhm
 * @create 2020-09-29 15:07
 */
object SparkSQL05_UDF {
  def main(args: Array[String]): Unit = {
   // 1. 创建SparkConf并设置App名称
   val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

   // 2 创建SparkSession对象
   val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 读取数据
    val df: DataFrame = spark.read.json("input/user.json")

    spark.udf.register("addName",(x:String)=>"Name:" + x)

    // 5 创建DataFrame临时视图
    df.createOrReplaceTempView("user")

    // 6 调用自定义UDF函数
    spark.sql("select addName(name),age from user").show()



   // 5 释放资源
   spark.stop()
  }
}
