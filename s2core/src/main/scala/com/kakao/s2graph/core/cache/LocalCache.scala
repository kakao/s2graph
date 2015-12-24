package com.kakao.s2graph.core.cache

import java.util.concurrent.{ConcurrentMap, TimeUnit}

import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config


case class LocalCache[K, V](config: Config) extends S2Cache[K, V] {
  import S2Cache._
  val maxSize = config.getInt(MaxSizeKey)
  val expireAfterWrite = config.getInt(ExpireAfterWriteKey)
  val expireAfterAccess = config.getInt(ExpireAfterAccessKey)
  val cache = CacheBuilder.newBuilder()
    .initialCapacity(maxSize)
    .maximumSize(maxSize)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
    .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
    .build[K, V]()

  override def getIfPresent(key: K): V = cache.getIfPresent(key)

  /** ignore cache ttl on local cache per key */
  override def putIfAbsent(key: K, value: V, cacheTTLOpt: Option[Long] = None): V = cache.asMap().putIfAbsent(key, value)

  override def invalidate(key: K): Unit = cache.invalidate(key)
}