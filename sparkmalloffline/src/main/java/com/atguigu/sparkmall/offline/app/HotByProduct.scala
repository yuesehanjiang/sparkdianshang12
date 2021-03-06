package com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.common.bean.ClickByProduct
import org.apache.spark.rdd.RDD
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
println(spark)
var sql=    " \n\t   select t2.*\n\t     from (select  t1.*,   rank() over(partition by area ," +
  " click_product_id  order by aa  desc) as ranks from haha  t1 ) t2 \n\n\t    where t2.ranks<=2"
    import spark.implicits._

    spark.sql("use  sparkmall")
    val rdd: RDD[ClickByProduct] = spark.sql(sql).as[ClickByProduct].rdd
   //点击数  商品ID     商品名字    城市
    //ClickByProduct(4,华东,23,苏州,70,90)
    //点击数   区域   city_iid   城市   商品ID    该商品的点击总数   小数 百分被
    rdd.foreach(println)


println("================================================",rdd.count())

    rdd.foreach(println)



  }


}
