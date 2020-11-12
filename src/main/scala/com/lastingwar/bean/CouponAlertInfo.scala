package com.lastingwar.bean

/**
 * 预警日志样例类
 * @author yhm
 * @create 2020-11-10 11:09
 */
case class CouponAlertInfo(mid:String,
                            uids:java.util.HashSet[String],
                            itemIds:java.util.HashSet[String],
                            events:java.util.List[String],
                            ts:Long
                          )
