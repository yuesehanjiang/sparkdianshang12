import java.util

import ClickCount.jeds
import com.atguigu.sparkmall.common.bean.AdsInfo
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import com.atguigu.sparkmall.common.util.MyKafkaUtil.{checkUserFromBlackList, jeds}
import com.atguigu.sparkmall.mock.util.ConfigurationUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

/**
  * Created by qzy017 on 2019/6/21.
  */
object ClickCount {

  val config = ConfigurationUtil("config.properties")
  val broker_list = config.getString("kafka.broker.list")
  var jeds=new Jedis("127.0.0.1",6379)
  jeds.select(10)
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
      val ssc = new StreamingContext(sc, Seconds(10))
      // 4. 得到 DStream
      val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "tc")
      // 5. 1561088764317,华东,杭州,103,5

      var aa=    recordDStream.map(record =>{
         val  split= record.value().split(",")
        (split(4),1L)

      })


      aa.foreachRDD(x=>{
       x.foreachPartition(x=>{
         x.foreach(
           x=>{
                 println(x)
              val x1=x._1.toString
                val x2=x._2.toLong


             println("hahha",x1,x2)
             jeds.hincrBy(x1,"user",1)  //数据累加1
          }



         )
       })
      })







      //读出黑名单

      ssc.start()
      ssc.awaitTermination()

    }

}
