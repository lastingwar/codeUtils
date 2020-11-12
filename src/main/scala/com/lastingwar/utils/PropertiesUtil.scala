package com.lastingwar.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author yhm
 * @create 2020-11-11 21:09
 */
object PropertiesUtil {
  def load(propertiesName:String):Properties ={
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
    prop
  }
}