package com.atguigu.sparkmall.mock.util

/**
  * Created by qzy017 on 2019/6/12.
  */
 object offline {

  def isNotEmpty(text: String): Boolean = text != null && text.length == 0

  def isEmpty(text: String): Boolean = !isNotEmpty(text)
}