package s2.models

import java.net.InetAddress

import com.twitter.util.SynchronizedLruMap
import org.slf4j.LoggerFactory

import scala.language.postfixOps

/**
 * Created by alec on 15. 2. 11..
 */
case class ConfigurableCacheConfig(maxSize: Int, ttl: Int, negativeCache: Boolean = false, negativeTTL: Int = 600)

case class CacheStats(var request: Long, var expired: Long, var hit: Long) {
  def copy(): CacheStats = synchronized { CacheStats(request, expired, hit) }
}

class ConfigurableCache[V](config: ConfigurableCacheConfig) {
  private lazy val cache = new SynchronizedLruMap[String, (Option[V], Int)](config.maxSize)
  private lazy val className = this.getClass.getSimpleName

  private lazy val log = LoggerFactory.getLogger(this.getClass)
  val localHostname = InetAddress.getLocalHost.getHostName

  def size = cache.size
  def maxSize = cache.maxSize

  def currentTime: Int = System.currentTimeMillis() / 1000 toInt

  // cache statistics
  private val stats = CacheStats(0, 0, 0)

  def getStats: CacheStats = stats.copy()

  def getStatsString: String = {
    val s = getStats
    s"$localHostname ${this.getClass.getSimpleName} size: $size / $maxSize hit_rate: ${s.hit} / ${s.request} = ${s.hit.toDouble / s.request}, expired: ${s.expired}"
  }

  private def withTTL(key: String, withStats: Boolean = true)(op: => Option[V]): Option[V] = {
    lazy val current: Int = currentTime

    if (withStats) stats.synchronized { stats.request += 1}

    val cached = cache.get(key).filter { case (existed, ts) =>
      val elapsed = current - ts
      if ((existed.nonEmpty && elapsed > config.ttl) || (existed.isEmpty && elapsed > config.negativeTTL)) {
        // expired
        cache.remove(key)
        if (withStats) stats.synchronized { stats.expired += 1}
        false
      } else {
        if (withStats) stats.synchronized { stats.hit += 1}
        // cache hit
        true
      }
    }.map(_._1)

    cached.getOrElse {
      val r = op
      if (r.nonEmpty || config.negativeCache) {
        cache.put(key, (r, current))
      }
      r
    }
  }

  def withCache(key: String)(op: => Option[V]): Option[V] = {
    withTTL(s"$className:$key")(op)
  }

  def purgeKey(key: String) = {
    if (cache.contains(key)) cache.remove(key)
  }

  def contains(key: String): Boolean = cache.contains(key)
}
