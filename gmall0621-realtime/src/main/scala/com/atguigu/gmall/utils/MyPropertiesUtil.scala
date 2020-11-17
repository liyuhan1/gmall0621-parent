package com.atguigu.gmall.utils

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

object MyPropertiesUtil {
  def load(propFileName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
      getResourceAsStream(propFileName), StandardCharsets.UTF_8))
    prop
  }
}
