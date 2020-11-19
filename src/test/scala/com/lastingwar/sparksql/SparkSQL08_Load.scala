package com.lastingwar.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author yhm
 * @create 2020-09-29 15:27
 */
object SparkSQL08_Load {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    spark.read.json("input/user.json").show()

    spark.read.format("json").load("input/user.json").show()


    spark.sql("select * from json.`input/user.json`").show()
    // 5 释放资源
    spark.stop()
  }
}
