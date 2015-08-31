package s2.models

import scalikejdbc.AutoSession

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 27..
 */
trait CachedDBModel[T] {
  implicit val s = AutoSession

  val cacheConfig: ConfigurableCacheConfig
  lazy val cache = new ConfigurableCache[T](cacheConfig)
}
