package com.lastingwar.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 * @author yhm
 * @create 2020-09-29 15:14
 */
object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/user.json")

    spark.udf.register("myAvg",functions.udaf(new MyAvgUDAF()))

    df.createOrReplaceTempView("user")

    spark.sql("select myAvg(age) from user").show()


    // 5 释放资源
    spark.stop()
  }

  case class User(age: Long, name: String)

  case class Buff(var sum: Long, var cnt: Long)


  class MyAvgUDAF extends Aggregator[User, Buff, Double] {

    override def zero: Buff = Buff(0L, 0L)

    override def reduce(b: Buff, a: User): Buff = {
      b.sum += a.age
      b.cnt += 1
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum += b2.sum
      b1.cnt += b2.cnt
      b1
    }

    override def finish(reduction: Buff): Double = {
      reduction.sum.toDouble / reduction.cnt
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}
