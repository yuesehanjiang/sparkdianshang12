/**
  * Created by qzy017 on 2019/6/21.
  * 一个小时的点击数的统计
  */
import java.text.SimpleDateFormat

import com.atguigu.sparkmall.common.bean.AdsInfo
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import com.atguigu.sparkmall.mock.util.ConfigurationUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsClickApp {


  val config = ConfigurationUtil("config.properties")
  val broker_list = config.getString("kafka.broker.list")
  var jeds=new Jedis("127.0.0.1",6379)
  jeds.select(4)
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
  def statLastHourAdsClick(filteredDStream: DStream[AdsInfo]) = {
    val windowDStream: DStream[AdsInfo] = filteredDStream.window(Minutes(60), Seconds(6))

    val groupAdsCountDStream: DStream[(String, Iterable[(String, Int)])] = windowDStream.map(adsInfo => {
      val houreMinutes = new SimpleDateFormat("HH:mm").format(adsInfo.timestamp)
      ((adsInfo.adid, houreMinutes), 1)
    }).reduceByKey(_ + _).map {
      case ((adsId, hourMinutes), count) => (adsId, (hourMinutes, count))
    }.groupByKey

    val jsonCountDStream: DStream[(String, String)] = groupAdsCountDStream.map {
      case (adsId, it) => {
        import org.json4s.JsonDSL._
        val hourMinutesJson: String = JsonMethods.compact(JsonMethods.render(it))
        (adsId, hourMinutesJson)
      }
    }

    jsonCountDStream.foreachRDD(rdd => {
      val result: Array[(String, String)] = rdd.collect
      import collection.JavaConversions._

      jeds.hmset("last_hour_ads_click", result.toMap)
      jeds.close()
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
    val ssc = new StreamingContext(sc, Seconds(50))
    // 4. 得到 DStream
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "tc")
    // 5. 1561088764317,华东,杭州,103,5

    val unit: DStream[String] = recordDStream.map(_.value())

    var  aa=unit.map(x=>{
      val strings: Array[String] = x.split(",")


      AdsInfo(strings(0).toLong,strings(1),strings(2),strings(3),strings(4))
    })



    statLastHourAdsClick(aa)

  }
}