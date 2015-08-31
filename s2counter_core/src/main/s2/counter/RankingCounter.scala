package s2.counter

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import s2.helper.HashShardingJedis

import scala.collection.JavaConverters._
//import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps



/**
 * Created by jay on 14. 11. 17..
 */
@deprecated("use core.RankingCounter", "0.14")
class RankingCounter(config: Config) {
  private[counter] val log = LoggerFactory.getLogger(this.getClass)
  private[counter] val withRedis = new HashShardingJedis(config)

  //mysql에 k저장할 때 caching을 위해 k는 prefix 별로 정해져야

  //TODO : temp_k -> 키별로 정할 수 있도록 변경 요
  val defaultK = 50
  /* var k = defaultK    */

  val K_KEY = "!^"

  def incrementCore(bucket : String, key : String, amount : Long = 1, isAccumulative : Boolean = true, k : Int = defaultK) = {
    withRedis.doBlockWithBucketName({jedis =>
      jedis.watch(bucket)
      val elementRank = jedis.zrank(bucket, key)
      val hasKey = elementRank != null

      //      //for Debug
      //      val keys = jedis.zrange(bucket, 0, -1)
      //      val count = jedis.zcard(bucket)
      //      Logger.debug(s"before keys : $keys, size : $count")
      //      assert(keys.size == k, s"current queue size : ${keys.size} if 0, please create_queue first else please contact system admin.") //please call create_group first

      if (k <= 0) { throw new Exception("please update K first")}

      val isNotFull = jedis.zcard(bucket) < k

      if(hasKey || isNotFull) {
        if (isAccumulative) {
          val ret = jedis.zincrby(bucket, amount, key)
//          Logger.debug(s"incr ret : $ret, bucket : $bucket, key : $key, amount : ${amount}")
          true
        } else {
          val ret = jedis.zadd(bucket, amount, key)
//          Logger.debug(s"add ret : $ret, bucket : $bucket, key : $key, amount : ${amount}")
          true
        }
      } else {              //replace if this key is not in queue
        val minElementKey = jedis.zrangeByScore(bucket, "-inf", "+inf", 0, 1).asScala.toList.head
        val minValue = jedis.zscore(bucket, minElementKey)
//        Logger.debug(s"min key: $minElementKey value : $minValue")
//        Logger.debug(s"min element to delete : $minElementKey")

        val transaction = jedis.multi()

        transaction.zrem(bucket, minElementKey)
        if(isAccumulative)
          transaction.zadd(bucket, minValue + amount, key)
        else
          transaction.zadd(bucket, amount, key)

        val ret = transaction.exec()
//        Logger.debug(s"incr isAccumulative:$isAccumulative transaction_ret : $ret, bucket : $bucket, key : $key, amount : ${amount}")
        ret != null // return value
      }
    }, {false}, bucket).asInstanceOf[Boolean]
  }
  
  def addValueToSet(key: String, value: String, limit: Int): Unit = {
    withRedis.doBlockWithKey(key) { jedis =>
      jedis.watch(key)
      val isFull = jedis.zcard(key) >= limit

      if (isFull) {
        throw new RuntimeException(s"$key set is full")
      }
      else {
        if (jedis.zrank(key, value) != null) {
          jedis.zadd(key, 0, value)
        }
      }
    } {
      // fallback
    }
  }

  def getValuesFromSet(key: String): Set[String] = {
    withRedis.doBlockWithKey(key) { jedis =>
      jedis.watch(key)
      jedis.zrangeByLex(key, "-", "+").asScala.toSet
    } {
      Set.empty[String]
    }
  }

  def incrementBulk(bucket : String, contents : List[(String, Long)], retryCount : Int = 0, isAccumulated : Boolean = true, k : Int = defaultK) = {
    val sortedRequest = contents.sortBy(_._2)

    for (e <- sortedRequest) {
      var tryLeft = retryCount + 1
      var ret = false
      while (tryLeft > 0 && !ret) {
        ret = incrementCore(bucket, e._1, e._2, isAccumulated, k)
        tryLeft -= 1
      }
    }
    true
  }

//  def incrementBulkWithFuture(bucket : String, contents : List[(String, Long)], retryCount : Int = 0, isAccumulated : Boolean = true, k : Int = defaultK, ex: ExecutionContext = multiGetContext) = {
//    val sortedRequest = contents.sortBy(_._2)
//
//    val futures = for (e <- sortedRequest) yield {
//      Future {
//        var tryLeft = retryCount + 1
//        var ret = false
//        while (tryLeft > 0 && !ret) {
//          ret = incrementCore(bucket, e._1, e._2, isAccumulated, k)
//          tryLeft -= 1
//        }
//      }(ex)
//    }
//    Await.result({
//      Future.sequence(futures)
//    }, 5 second)
//    true
//  }

  def redisSet(k : String, v: String) = {
    withRedis.doBlockWithBucketName({
      jedis =>
        jedis.set(K_KEY + k, v)
    }, {
      false
    }, k)
  }

  def redisGet(k : String, v: String) = {
    withRedis.doBlockWithBucketName({
      jedis =>
        jedis.set(K_KEY + k, v)
    }, {
      false
    }, k)
  }

  def redisIncr(k : String, v: Long) = {
    withRedis.doBlockWithBucketName({
      jedis =>
        jedis.incrBy(K_KEY + k, v.toLong)
    }, {
      false
    }, k)
  }


  def redisDelete(k : String) = {
    withRedis.doBlockWithBucketName({
      jedis =>
        jedis.set(K_KEY + k, "")
    }, {
      false
    }, k)
  }

  def redisGetSet(k : String, v : String) = {
    withRedis.doBlockWithBucketName({
      jedis =>
        jedis.getSet(K_KEY + k, v)
    }, {
      false
    }, k)
  }

  def getTopK(bucket : String, k : Int = Int.MaxValue): List[(String, Double)] = {
    withRedis.doBlockWithKey(bucket) { jedis =>
      jedis.zrevrangeByScoreWithScores(bucket, "+inf", "-inf", 0, k).asScala.toList.map(t => (t.getElement, t.getScore))
    } {
      List.empty[(String, Double)]
    }
  }

  def getTopKAsync(bucket : String, k : Int = Int.MaxValue)(implicit ec: ExecutionContext): Future[List[(String, Double)]] = {
    Future {
      val rst = withRedis.doBlockWithBucketName({jedis =>
        jedis.zrevrangeByScoreWithScores(bucket, "+inf", "-inf", 0, k).asScala.toList.map(t => (t.getElement, t.getScore))
      }, {List[(String, Double)]()}, bucket)
      rst.asInstanceOf[List[(String, Double)]]
    }
  }

  def getExactNumber(bucket : String, key : String) = {
    withRedis.doBlockWithBucketName({jedis =>
      val score = jedis.zscore(bucket, key)
      if (score == null) {
        val minElementKey = jedis.zrangeByScore(bucket, "+inf", "-inf", 0, 1).asScala.toList.head
        val minValue = jedis.zrank(bucket, minElementKey)
        Some(minValue)
      } else{
        Some(score)
      }
    }, None, bucket).asInstanceOf[Option[Double]]
  }
}
