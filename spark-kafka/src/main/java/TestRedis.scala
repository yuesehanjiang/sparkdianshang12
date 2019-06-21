import com.atguigu.sparkmall.mock.util.ConfigurationUtil
import redis.clients.jedis.Jedis

/**
  * Created by qzy017 on 2019/6/21.
  */
object TestRedis {

  val config = ConfigurationUtil("config.properties")
  val broker_list = config.getString("kafka.broker.list")
  var jeds=new Jedis("127.0.0.1",6379)
  jeds.select(11)
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 100) {

      jeds.hincrBy("user", "user", 100)
    }
  }
}
