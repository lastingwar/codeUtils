package com.lastingwar.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author yhm
 * @create 2020-09-29 15:02
 */
object SparkSQL04_DataFrameAndDataSet {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3 读取数据
    val df: DataFrame = spark.read.json("input/user.json")
    val userDataSet: Dataset[User] = df.as[User]
    userDataSet.show()


    // 4.3 DataSet 转换为DataFrame
    val userDataFrame: DataFrame = userDataSet.toDF()

    userDataFrame.show()

    // 5 释放资源
    spark.stop()
  }
}
