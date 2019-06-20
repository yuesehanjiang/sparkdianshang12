package com.atguigu.sparkmall.common.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by qzy017 on 2019/6/20.
  *
  * 1561019772850,华东,CityInfo(2,上海,华东),103,5
  * 1561019772860,华北,CityInfo(7,天津,华北),103,5
  **
  *array += s"$timestamp,$area,$city_name,$userid,$adid"
  */
case   class AdsInfo(timestamp:Long,area:String,city_name:String,userid:String,adid:String) {
    val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp))
    override def toString: String = s"$dayString:$area:$city_name:$adid"
}
