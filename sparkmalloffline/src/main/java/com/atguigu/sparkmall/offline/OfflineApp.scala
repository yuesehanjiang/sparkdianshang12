package com.atguigu.sparkmall.offline

/**
  * Created by qzy017 on 2019/6/12.
  */

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.mock.util.{ConfigurationUtil, offline}
import com.atguigu.sparkmall.offline.app.CategoryTop10App
import com.atguigu.sparkmall.common.bean.Condition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//读取hive 中的数据
object OfflineApp {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("OfflineApp")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://z101:9000/user/hive/warehouse")
      .getOrCreate()
    val taskId = UUID.randomUUID().toString
    // 根据条件过滤取出需要的 RDD, 过滤条件定义在配置文件中
      val value: RDD[UserVisitAction] = readUserVisitActionRDD(spark,readConditions)


    println("任务1: 开始")
    CategoryTop10App.statCategoryTop10(spark, value, taskId)
    println("任务1: 结束")

    println(readConditions)

  }

  /**
    * 读取指定条件的 UserVisitActionRDD
    *
    * @param spark
    *
    */
  def readUserVisitActionRDD(spark: SparkSession, condition: Condition): RDD[UserVisitAction] = {
    var sql = s"select v.* ,u.age from user_visit_action v left join user_info u on u.user_id=v.user_id  where 1=1"
    if (offline.isNotEmpty(condition.startDate)) {
      sql += s" and v.date>='${condition.startDate}'"
    }
    if (offline.isNotEmpty(condition.endDate)) {
      sql += s" and v.date<='${condition.endDate}'"
    }

    if (condition.startAge != 0) {
      sql += s" and u.age>=${condition.startAge}"
    }
    if (condition.endAge != 0) {
      sql += s" and u.age<=${condition.endAge}"
    }
    sql += s" limit 1000"
    println("sql",sql)
    import spark.implicits._
    spark.sql("use sparkmall")

    val rdd: RDD[UserVisitAction] = spark.sql(sql).as[UserVisitAction].rdd

   // spark.sql(sql).show()
    rdd
  }

  /**
    * 读取过滤条件
    *
    * @return
    */
  def readConditions: Condition = {


    println(23423423)
    // 读取配置文件
    val config = ConfigurationUtil("conditions.properties")
    // 读取到其中的 JSON 字符串
    val conditionString = config.getString("age")

    println("aaa",conditionString)
    // 解析成 Condition 对象
    JSON.parseObject(conditionString, classOf[Condition])

  }
}
