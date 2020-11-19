package com.lastingwar.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * @author yhm
 * @create 2020-09-29 15:41
 */
object SparkSQL11_MySQL_Write {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val users = List(User(10000, "zhangsan"), User(100100, "lisi"))
    val rdd: RDD[User] = spark.sparkContext.makeRDD(users)
    val ds: Dataset[User] = rdd.toDS()
    ds.write
        .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .mode(SaveMode.Append)
      .save()


    // 5 释放资源
    spark.stop()
  }
  case class User(id: Int, name: String)
}
