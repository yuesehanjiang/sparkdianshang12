package com.atguigu.sparkmall.mock.util

import com.atguigu.sparkmall.common.bean.ClickByProduct
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
println(spark)
var sql=    "select count(uv.click_product_id) click_num ," +
  "uv.click_product_id," +
  "tpi.product_name,uv.city_id " +
  "from sparkmall.user_visit_action uv " +
  "left join sparkmall.product_info  pi on pi.product_id=uv.click_product_id" +
  "left join sparkmall.city_info " +
  " c on c.city_id=uv.city_id where uv.click_product_id !=-1 " +
  "GROUP by uv.click_product_id,pi.product_name,uv.city_id order by uv.click_product_id desc"

    import spark.implicits._
    val rdd: RDD[ClickByProduct] = spark.sql(sql).as[ClickByProduct].rdd


    rdd.foreach(println)

 /*   aa:Long,area:String,city_id:Long,city_name:String,
    click_product_id:Long,countall:Long*/






  }


}
