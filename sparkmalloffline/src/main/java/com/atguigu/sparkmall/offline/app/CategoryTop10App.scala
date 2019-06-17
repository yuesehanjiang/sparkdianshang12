package com.atguigu.sparkmall.offline.app

/**
  * Created by qzy017 on 2019/6/12.
  */
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.mock.util.JDBCUtil
import com.atguigu.sparkmall.offline.acc.MapAccumulator
import com.atguigu.sparkmall.common.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTop10App {

  // 统计热门品 Top10
  def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {

    println(123)
    println(123)
    println(123)
    // 1. 注册累加器
    val acc = new MapAccumulator
    spark.sparkContext.register(acc, "dfsfd")

    // 2. 遍历日志
   var aa= userVisitActionRDD.foreach {
      visitAction => {
        if (visitAction.click_category_id != -1) {
          acc.add(visitAction.click_category_id.toString, "click")
        } else if (visitAction.order_category_ids != null) {
          visitAction.order_category_ids.split(",").foreach {
            oid => acc.add(oid, "order")
          }
        } else if (visitAction.pay_category_ids != null) {
          visitAction.pay_category_ids.split(",").foreach {
            pid => acc.add(pid, "pay")
          }
        }
      }
    }



    // 3. 遍历完成之后就得到每个每个品类 id 和操作类型的数量.  然后按照 CategoryId 进行进行分组

    // ((20,pay),2)  ===>(16,Map((16,order) -> 1, (16,pay) -> 2))

    val actionCountByCategoryIdMap = acc.value.groupBy(_._1._1)

    actionCountByCategoryIdMap.foreach(println)

    // 4. 聚合成 CategoryCountInfo 类型的集合
    val categoryCountInfoList = actionCountByCategoryIdMap.map {
      case (cid, actionMap) => CategoryCountInfo(
        taskId,
        cid,
        actionMap.getOrElse((cid, "click"), 0),
        actionMap.getOrElse((cid, "order"), 0),
        actionMap.getOrElse((cid, "pay"), 0)
      )
    }.toList


    println("开始排序 ....................")

    categoryCountInfoList.foreach(println)
    // 5. 按照 点击 下单 支付 的顺序降序来排序
    val sortedCategoryInfoList = categoryCountInfoList.sortBy(info => (info.clickCount))

    println("为===============难")

    sortedCategoryInfoList.foreach(println)

    // 6. 截取前 10
    val top10 = sortedCategoryInfoList.take(10)

    // 7. 插入数据库
    val argsList = top10.map(info => Array(info.taskId, info.categoryId, info.clickCount, info.orderCount, info.payCount))
    JDBCUtil.executeBatchUpdate("insert into category_top10 values(?, ?, ?, ?, ?)", argsList)
  }

}

/*
1. 遍历全部日志表, 根据品类 id 和操作类型分别累加. 需要用到累加器

    - 定义累加器
        累加器用什么来保存? map
         key:  (categoryId, action)    元组来表示
         value: count
    - 当碰到订单和支付业务的时候注意拆分字段才能得到品类 id

2. 遍历完成之后就得到每个每个品类 id 和操作类型的数量. 然后按照 cid 进行聚合, 聚合成 CategoryCountInfo 类型

3. 按照 点击 下单 支付 的顺序来排序

4. 取出 Top10

5. 写入到 Mysql 数据库
*/