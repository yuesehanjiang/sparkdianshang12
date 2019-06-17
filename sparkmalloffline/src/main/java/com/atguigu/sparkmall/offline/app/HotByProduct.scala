package com.atguigu.sparkmall.offline.app

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by qzy017 on 2019/6/17.
  *
  *计算热门前三
  *
  *
  * select count(uv.click_product_id),ci.city_id,ci.city_name  from hive.sparkmall.user_visit_action  uv
  * left join hive.sparkmall.city_info   ci on ci.city_id=uv.city_id
  * group by ci.city_name,ci.city_id
  */
object HotByProduct {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("OfflineApp")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "hdfs://z101:9000/user/hive/warehouse")
    .getOrCreate()


  def main(args: Array[String]): Unit = {

   var sql="select count(uv.click_product_id) click_num,ci.city_id,ci.city_name,uv.click_product_id " +
     "from user_visit_action  uv" +
     "left join city_info   ci on ci.city_id=uv.city_id " +
     "where uv.click_product_id !=-1" +
     "group by ci.city_name,ci.city_id  ,uv.click_product_id"
    val frame: DataFrame = spark.sql(sql)





  }


}
