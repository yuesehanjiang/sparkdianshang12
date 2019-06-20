/**
  * Created by qzy017 on 2019/6/20.
  */
import java.util.Properties

import com.atguigu.sparkmall.common.bean.{CityInfo, ProductInfo, UserInfo, UserVisitAction}
import com.atguigu.sparkmall.mock.util.{RandomNumUtil, RandomOptions}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
/**
  * 生成实时的模拟数据
  */
object MockRealTime {


  def mockCityInfo: List[CityInfo] = {
    List(CityInfo(1L, "北京", "华北"),
      CityInfo(2L, "上海", "华东"),
      CityInfo(3L, "深圳", "华南"),
      CityInfo(4L, "广州", "华南"),
      CityInfo(5L, "武汉", "华中"),
      CityInfo(6L, "南京", "华东"),
      CityInfo(7L, "天津", "华北"),
      CityInfo(8L, "成都", "西南"),
      CityInfo(9L, "哈尔滨", "东北"),
      CityInfo(10L, "大连", "东北"),
      CityInfo(11L, "沈阳", "东北"),
      CityInfo(12L, "西安", "西北"),
      CityInfo(13L, "长沙", "华中"),
      CityInfo(14L, "重庆", "西南"),
      CityInfo(15L, "济南", "华东"),
      CityInfo(16L, "石家庄", "华北"),
      CityInfo(17L, "银川", "西北"),
      CityInfo(18L, "杭州", "华东"),
      CityInfo(19L, "保定", "华北"),
      CityInfo(20L, "福州", "华南"),
      CityInfo(21L, "贵阳", "西南"),
      CityInfo(22L, "青岛", "华东"),
      CityInfo(23L, "苏州", "华东"),
      CityInfo(24L, "郑州", "华北"),
      CityInfo(25L, "无锡", "华东"),
      CityInfo(26L, "厦门", "华南"))
  }
  /*
  数据格式:
      timestamp   area    city    userid  adid
      某个时间点 某个地区  某个城市   某个用户  某个广告

   */
  def mockRealTimeData() = {
    // 存储模拟的实时数据
    val array = ArrayBuffer[String]()
    // 城市信息
   /* 西南,CityInfo(14,重庆,西南),104,4
    1561019819456,西南,CityInfo(8,成都,西南),100,2
    1561019819467,西北,CityInfo(17,银川,西北),104,4*/


    (1 to 50).foreach {
      i => {
        val timestamp = System.currentTimeMillis()
        var i = RandomNumUtil.randomInt(1, 25)
        val citys = mockCityInfo
        val city: CityInfo = citys(i)


         val area=city.area;
        val city_name = city.city_name
        val userid = RandomNumUtil.randomInt(100, 105)
        val adid = RandomNumUtil.randomInt(1, 5)
        array += s"$timestamp,$area,$city_name,$userid,$adid"
        Thread.sleep(10)
      }
    }
    array
  }

  def createKafkaProducer: KafkaProducer[String, String] = {
    val props = new Properties
    // Kafka服务端的主机名和端口号
    props.put("bootstrap.servers", "z101:9092,z102:9092,z103:9092")
    // 等待所有副本节点的应答
    props.put("acks", "1")
    // 重试最大次数
    props.put("retries", "0")

    // 批消息处理大小
    props.put("batch.size", "16384")
    // 请求延时
    props.put("linger.ms", "1")
    //// 发送缓存区内存大小
    props.put("buffer.memory", "33554432")
    // key序列化
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  def main(args: Array[String]): Unit = {
    val topic = "tc"
    val producer: KafkaProducer[String, String] = createKafkaProducer
    while (true) {
      mockRealTimeData().foreach {
        msg => {
          producer.send(new ProducerRecord(topic, msg))
          Thread.sleep(100)
        }
      }
      Thread.sleep(1000)
    }
  }
}