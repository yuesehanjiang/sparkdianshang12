import java.text.SimpleDateFormat
import java.util.Date

import ClickCount.jeds
import com.atguigu.sparkmall.common.bean.AdsInfo
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import com.atguigu.sparkmall.mock.util.ConfigurationUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Created by qzy017 on 2019/6/21.
  *
  * 对定时Stream的 另外一种解析方式
  */
object ClickByEveryDayEverEare {

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

    val value: DStream[String] = recordDStream.map(_.value())

    val aa = value.map(x => {
      val strings: Array[String] = x.split(",")


      val info: AdsInfo = AdsInfo(strings(0).toLong,strings(1),strings(2),strings(3),strings(4))
      val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(info.timestamp))
      (s"${dayString}:${info.area}:${info.adid}",1L)
    })

/*    (2019-06-21:西南:2,14)
    (2019-06-21:华北:5,25)
    (2019-06-21:西北:2,6)
    (2019-06-21:华东:4,27)
    (2019-06-21:东北:3,10)
    (2019-06-21:东北:5,15)
    (2019-06-21:华东:5,23)*/




    val bb= aa.reduceByKey(_+_).map(x=>{
       val area: String = x._1.split(":")(1)
       val adid: String = x._1.split(":")(2)

       (area,(adid,x._2))



     }).groupByKey().map(x=>{
      val tuples: List[(String, Long)] = x._2.toList.sortWith(_._2 > _._2).take(2)
      import org.json4s.JsonDSL._ // 加载的隐式转换  json4s  是面向 scala 的 json 转换
      val adsCountJson: String = JsonMethods.compact(JsonMethods.render(tuples))

      (x._1,adsCountJson)
    }).map(x=>{
      //jeds.set
    })





    //只有循环才可以争取答应出来

    //读出黑名单

    ssc.start()
    ssc.awaitTermination()

  }
  }
