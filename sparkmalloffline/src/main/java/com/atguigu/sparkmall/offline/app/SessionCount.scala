package com.atguigu.sparkmall.offline.app

import java.util.UUID

import com.atguigu.sparkmall.common.bean.{CategoryTop10SessionCount, UserVisitAction}
import com.atguigu.sparkmall.mock.util.JDBCUtil
import com.atguigu.sparkmall.offline.OfflineApp
import com.atguigu.sparkmall.offline.OfflineApp.{readConditions, readUserVisitActionRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable



/**
  * Created by qzy017 on 2019/6/13.
  */
object SessionCount {


  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("OfflineApp")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "hdfs://z101:9000/user/hive/warehouse")
    .getOrCreate()





  def sessionCount(sparkSession: SparkSession)={

    //读取数据
    val value: RDD[UserVisitAction] = OfflineApp.readUserVisitActionRDD(sparkSession,OfflineApp.readConditions)

  var aa=value.filter(x=>x.click_category_id != -1).map{
        x=> CategoryTop10SessionCount("123",x.click_category_id,x.session_id,1)
   }

    val value1: RDD[((String, Long), Iterable[CategoryTop10SessionCount])] = aa.groupBy(x=>(x.sessionId,x.categoryId))
   println("转换前")
    value1.foreach(println)

    val value2: RDD[((String, Long), Int)] = value1.map(x=>(x._1,x._2.size))


    val value3: RDD[((String, Long), Int)] = value2.reduceByKey(_+_)
    println("转换后")
    value2.foreach(println)

   println(value2.count())

    println("最终的结果")
    value3.foreach(println)

    println(value3.count())
    import spark.implicits._


    var bb= value3.map{
      x=>CategoryTop10SessionCount("1",x._1._2,x._1._1,x._2)
     }


    //bb.sortBy(x=>(x.clickCount)(Ordering.Int.reverse))
    var dd= bb.collect().sortWith({
       case(p1,p2)=>p1>p2
     }).take(20)

   var cc= dd.map{
      x=>Array(x.taskId,x.categoryId,x.sessionId,x.clickCount)
    }
   println("排序后")
    cc.foreach(println)

    var  sql="INSERT INTO `sparkmall`.`category_top10_session_count` (  `taskId`,  `categoryId`,  `sessionId`, `clickCount`) VALUES " +
      "(?,?,?,? )"



    JDBCUtil.executeBatchUpdate(sql,cc)




  }

  def main(args: Array[String]): Unit = {
    sessionCount(spark)
  }



}
