package com.atguigu.sparkmall.offline.acc

/**
  * Created by qzy017 on 2019/6/12.
  */
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
/*
* - 定义累加器
        累加器用什么来保存? map
         key:  (categoryId, action)    元组来表示
         value: count
    - 当碰到订单和支付业务的时候注意拆分字段才能得到品类 id


    AccumulatorV2[(String, String)  输入类型 (品类   action)

   mutable.Map[(String, String), Long]  输出
* */
class MapAccumulator extends AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] {
  val map = mutable.Map[(String, String), Long]()
  //判断是不是为空
  override def isZero: Boolean = map.isEmpty
  //copy 数据  new 一个map ，把原来的map添加到新的map 中
  override def copy(): AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] = {
    val newAcc = new MapAccumulator
    map.synchronized {
      newAcc.map ++= map
    }
    newAcc
  }

  //重置
  override def reset(): Unit = map.clear

//累加器
  override def add(v: (String, String)): Unit = {
    map(v) = map.getOrElseUpdate(v, 0) + 1


  }

  // otherMap: (1, click) -> 20  this: (1, click) -> 10         thisMap: (1,2) -> 30
  // otherMap: (1, order) -> 5                                  thisMap: (1,3) -> 5

  //合并
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[(String, String), Long ]]): Unit = {
    val otherMap: mutable.Map[(String, String), Long] = other.value
    otherMap.foreach {
      kv => map.put(kv._1, map.getOrElse(kv._1, 0L) + kv._2)
    }

    otherMap.foreach(println)
  }

  //最终返回数据
  override def value(): mutable.Map[(String, String), Long] = {
    println("我是map",map)
    map

  }


}