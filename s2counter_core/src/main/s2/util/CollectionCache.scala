package s2.util

import java.net.InetAddress

import com.twitter.util.SynchronizedLruMap
import org.slf4j.LoggerFactory
import s2.models.ConfigurableCacheConfig

import scala.language.{postfixOps, reflectiveCalls}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 7. 1..
 */
class CollectionCache[C <: { def nonEmpty: Boolean; def isEmpty: Boolean } ](config: ConfigurableCacheConfig) {
  case class CacheStats(var request: Long, var expired: Long, var hit: Long) {
    def copy(): CacheStats = synchronized { CacheStats(request, expired, hit) }
  }

  private lazy val cache = new SynchronizedLruMap[String, (C, Int)](config.maxSize)
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

  private def withTTL(key: String, withStats: Boolean = true)(op: => C): C = {
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

  def withCache(key: String)(op: => C): C = {
    withTTL(s"$className:$key")(op)
  }

  def purgeKey(key: String) = {
    if (cache.contains(key)) cache.remove(key)
  }

  def contains(key: String): Boolean = cache.contains(key)
}
