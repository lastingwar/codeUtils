package com.lastingwar.sparksql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * 这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，
 * 并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示
 * @author yhm
 * @create 2020-11-17 19:58
 */
object SparkSQL12_TopN {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use default")

    // 0 注册自定义聚合函数
    spark.udf.register("city_remark", functions.udaf(new CityRemarkUDAF()))

    // 1. 查询出所有的点击记录,并和城市表产品表做内连接
    spark.sql(
      """
        |select
        |    v.click_product_id,
        |    c.area,
        |    c.city_name,
        |    p.product_name
        |from user_visit_action v
        |join city_info c
        |on v.city_id = c.city_id
        |join product_info p
        |on v.click_product_id = p.product_id
        |where click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    // 2. 计算每个区域, 每个产品的点击量
    spark.sql(
      """
        |select
        |    t1.area,
        |    t1.product_name,
        |    count(*) click_count,
        |    city_remark(t1.city_name)
        |from t1
        |group by t1.area, t1.product_name
            """.stripMargin).createOrReplaceTempView("t2")

    // 3. 对每个区域内产品的点击量进行倒序排列
    spark.sql(
      """
        |select
        |    *,
        |    rank() over(partition by t2.area order by t2.click_count desc) rank
        |from t2
            """.stripMargin).createOrReplaceTempView("t3")

    // 4. 每个区域取top3
    spark.sql(
      """
        |select
        |    *
        |from t3
        |where rank <= 3
            """.stripMargin).show

    // 5. 释放资源
    spark.stop()
  }

  case class Buffer(var totalcnt:Long, var cityMap: mutable.Map[String, Long])

  /**
   * IN: 城市名称 String
   * BUFF: Map[(cityName, 点击数量)], totalcnt
   * OUT:城市备注 String
   */
  class CityRemarkUDAF extends Aggregator[String, Buffer,String]{

    override def zero: Buffer = {
      Buffer(0L,mutable.Map[String, Long]())
    }

    override def reduce(buffer: Buffer, city: String): Buffer = {

      buffer.totalcnt += 1

      val newCount = buffer.cityMap.getOrElse(city, 0L) + 1

      buffer.cityMap.update(city, newCount)

      buffer
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {

      // 合并所有城市的点击数量的总和
      b1.totalcnt += b2.totalcnt

      // 合并城市Map(2个Map合并)
      val map1 = b1.cityMap
      val map2 = b2.cityMap

      map2.foreach{
        case(city, cnt) =>{
          val newCnt = map1.getOrElse(city, 0L) + cnt
          map1.update(city, newCnt)
        }
      }

      b1.cityMap = map1
      b1
    }

    // 计算结果：字符串（每个城市的点击比率）
    override def finish(buffer: Buffer): String = {

      val remarkList = ListBuffer[String]()

      val totalcnt: Long = buffer.totalcnt

      val cityMap = buffer.cityMap

      // 将统计的城市点击数量的集合进行排序
      val cityCountList: List[(String, Long)] = buffer.cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      // 判断是否存在其他的城市
      val hasOtherCity = cityMap.size > 2

      var sum = 0L

      cityCountList.foreach{
        case (city, cnt) =>{
          val r = cnt * 100 / totalcnt
          remarkList.append(city +" " + r + "%")
          sum += r
        }
      }

      // 判断是否存在其他的城市
      if (hasOtherCity){
        remarkList.append("其他 "+ (100 - sum) +"%")
      }

      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
