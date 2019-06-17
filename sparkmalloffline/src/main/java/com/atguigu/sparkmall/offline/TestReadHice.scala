package com.atguigu.sparkmall.offline

import com.atguigu.sparkmall.common.bean.{UserInfo, UserVisitAction}
import com.atguigu.sparkmall.offline.acc.MapAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by qzy017 on 2019/6/13.
  */
object TestReadHice {

  val sparkSession:SparkSession= SparkSession
  .builder()
    .master("local[*]")
    .appName("OfflineAppdfds")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "hdfs://z101:9000/user/hive/warehouse")
    .getOrCreate()





  def  read(sparkSession: SparkSession):  RDD[UserVisitAction] ={

    var sql="select * from user_visit_action limit 100"

    import sparkSession.implicits._
    sparkSession.sql("use sparkmall")
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql).as[UserVisitAction].rdd

    rdd

  }



  def jiexi(action:RDD[UserVisitAction]):Unit={
       action.foreach{
        action=>(
          if(action.city_id==14){
            println(action)
          }
        )
       }
  }





  def add(v: (String, String)): Unit = {
    var map=mutable.Map(("1","1")->2);
    var mm=map.getOrElseUpdate(v,0) + 1
    println(mm)

  }


  def main(args: Array[String]): Unit = {
    var unit: Unit = add(("1", "1000"))



  }


}
