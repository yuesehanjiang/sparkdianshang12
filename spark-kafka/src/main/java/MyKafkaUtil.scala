/**
  * Created by qzy017 on 2019/6/20.
  */
package com.atguigu.sparkmall.common.util

import java.util

import com.atguigu.sparkmall.common.bean.AdsInfo
import com.atguigu.sparkmall.mock.util.ConfigurationUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object MyKafkaUtil {
  val config = ConfigurationUtil("config.properties")
  val broker_list = config.getString("kafka.broker.list")
  var jeds=new Jedis("127.0.0.1",6379)
  // kafka消费者配置
  val kafkaParam = Map(
    "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "commerce-consumer-group",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量. 本次我们仍然自动维护偏移量
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )




  /*
   创建DStream，返回接收到的输入数据
   LocationStrategies：根据给定的主题和集群地址创建consumer
   LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
   ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
   ConsumerStrategies.Subscribe：订阅一系列主题
   */

  def getDStream(ssc: StreamingContext, topic: String) = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, // 标配. 只要 kafka 和 spark 没有部署在一台设备就应该是这个参数
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
  }



  // 过滤黑名单中的用户. 返回值是不包含黑名单的用户的广告点击记录的 DStream
  def checkUserFromBlackList(adsClickInfoDStream: DStream[AdsInfo], sc: SparkContext): DStream[AdsInfo] = {
    //1 拦截数据 过率数据
         adsClickInfoDStream.transform(rdd => {
      // 读出来黑名单
      val blackList: util.Set[String] = jeds.smembers("hei")
      val blackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackList)
      jeds.close()
      rdd.filter(adsInfo => {
        !blackListBC.value.equals(adsInfo.userid)
      })
    })
  }


    def main(args: Array[String]): Unit = {
      // 1. 创建 SparkConf 对象
      val conf: SparkConf = new SparkConf()
        .setAppName("RealTimeApp")
        .setMaster("local[*]")
      // 2. 创建 SparkContext 对象
      val sc = new SparkContext(conf)
      // 3. 创建 StreamingContext
      val ssc = new StreamingContext(sc, Seconds(1))
      // 4. 得到 DStream
      val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "tc")

      // 5. 为了方便后面的计算, 把消费到的字符串封装到对象中第三方第三方法
      val adsInfoDStream: DStream[AdsInfo] = recordDStream.map {
        record =>
          val split: Array[String] = record.value.split(",")
          AdsInfo(split(0).toLong, split(1), split(2), split(3), split(4))
      }


      println(123)

      val value: DStream[AdsInfo] = checkUserFromBlackList(adsInfoDStream: DStream[AdsInfo], sc: SparkContext)
      println(234)
//读取数据
      value.foreachRDD(rdd=>{
       // println("rdd",rdd)
        rdd.foreachPartition(x=>{//println("x",x)
        x.foreach(y=>{//println("yyyy",y.area,y.city_name,y.dayString,y.userid)
           val filed=s"${y.userid}:${y.dayString}:${y.adid}"
            jeds.hincrBy(s"${y.userid}",filed,1)  //数据累加1
            //如果点击大于100   就是黑名单
            val str: String = jeds.hget(s"${y.userid}",filed)
            if(str.toInt>=100){
              jeds.sadd("hei",s"${y.userid}")
            }
          }
          )
        }
        )
      })






      //读出黑名单






      ssc.start()
      ssc.awaitTermination()
    }


}