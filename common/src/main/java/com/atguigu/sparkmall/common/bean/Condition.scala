package com.atguigu.sparkmall.common.bean

/**
  * Created by qzy017 on 2019/6/12.
  */
case class Condition(var startDate: String,
                     var endDate: String,
                     var startAge: Int,
                     var endAge: Int,
                     var professionals: String,
                     var city: String,
                     var gender: String,
                     var keywords: String,
                     var categoryIds: String,
                     var targetPageFlow: String)



case class CategoryCountInfo(taskId: String,
                             categoryId: String,
                             clickCount: Long,
                             orderCount: Long,
                             payCount: Long)



