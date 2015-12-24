package com.kakao.s2graph.core.cache

import com.typesafe.config.Config
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

abstract class RedisCache[K, V](config: Config) extends S2Cache[K, V] {

  import S2Cache._

  val DefaultPort = 6379

  val RedisInstances = config.getStringList("redis.instances").map { s =>
    val sp = s.split(':')
    (sp(0), if (sp.length > 1) sp(1).toInt else DefaultPort)
  }

  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(150)
  poolConfig.setMaxIdle(50)
  poolConfig.setMaxWaitMillis(200)


  val jedisPools = RedisInstances.map { case (host, port) =>
    new JedisPool(poolConfig, host, port)
  }

  private def bucketIdx(key: String): Int = {
    MurmurHash3.stringHash(key) % jedisPools.size
  }

  private def withIdx[T](idx: Int)(f: Jedis => T): T = {
    val pool = jedisPools(idx)

    var jedis: Jedis = null

    try {
      jedis = pool.getResource

      f(jedis)
    }
    catch {
      case e: JedisException =>
        pool.returnBrokenResource(jedis)

        jedis = null
        throw e
    }
    finally {
      if (jedis != null) {
        pool.returnResource(jedis)
      }
    }
  }

  private def withKey[T](key: String)(f: Jedis => T): T = {
    withIdx(bucketIdx(key))(f)
  }

  def serialize(value: V): String
  def deserialize(value: String): V

  override def getIfPresent(key: K): V = withKey(key.toString) { jedis =>
    deserialize(jedis.get(key.toString))
  }

  override def invalidate(key: K): Unit = withKey(key.toString) { jedis =>
    jedis.del(key.toString())
  }

  /** strictly speaking we have to use transaction on this key.
    * but what we lose here without transaction is firing multipe RPC to same cache key
    * which is not a big problem.
    * */
  override def putIfAbsent(key: K, value: V, cacheTTLOpt: Option[Long] = None): V = withKey(key.toString) { jedis =>
    val newVal = serialize(value)
    jedis.setnx(key.toString, newVal) match {
      case 0 => //not set
        deserialize(jedis.get(key.toString()))
      case 1 => //set
        cacheTTLOpt match {
          case Some(cacheTTL) => jedis.expire(key.toString, cacheTTL.toInt)
          case None =>
        }
        value
    }
  }
}
