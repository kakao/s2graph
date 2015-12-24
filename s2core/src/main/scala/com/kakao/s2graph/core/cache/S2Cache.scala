package com.kakao.s2graph.core.cache


import java.util.concurrent.ConcurrentMap

object S2Cache {
  val MaxSizeKey = "cache.max.size"
  val ExpireAfterWriteKey = "cache.expire.after.write"
  val ExpireAfterAccessKey = "cache.expire.after.access"
}

trait S2Cache[K, V] {
  def getIfPresent(key: K): V
  def invalidate(key: K): Unit
  def putIfAbsent(key: K, value: V, cacheTTLOpt: Option[Long] = None): V
}


