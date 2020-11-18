package com.lastingwar.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author yhm
 * @create 2020-09-29 15:32
 */
object SparkSQL09_Save {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = spark.read.json("input/user.json")

//    df.write.save("output")

    spark.read.load("output").show()
    // 5 释放资源
    spark.stop()
  }
}
