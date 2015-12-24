package com.kakao.s2graph.core.cache

import java.util.concurrent.ConcurrentMap

import com.typesafe.config.Config
import scala.collection.JavaConversions._

class RedisCache[K, V](config: Config) extends S2Cache[K, V] {
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

  override def getIfPresent(key: K): V = ???

  override def asMap(): ConcurrentMap[K, V] = ???

  override def put(key: K, value: V): Unit = ???

  override def invalidate(key: K): Unit = ???
}
