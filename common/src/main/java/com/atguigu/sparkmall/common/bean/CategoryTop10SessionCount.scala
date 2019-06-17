package com.atguigu.sparkmall.common.bean

/**
  * Created by qzy017 on 2019/6/13.
  */
case class CategoryTop10SessionCount (taskId:String,  categoryId:Long,  sessionId:String,  clickCount:Int ) extends Ordered[CategoryTop10SessionCount] {
  override def compare(that: CategoryTop10SessionCount): Int = this.clickCount-that.clickCount
}
