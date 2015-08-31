package s2.models

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import s2.config.S2CounterConfig
import scalikejdbc._

/**
 * Created by alec on 15. 3. 31..
 */
class LabelModel(config: Config) extends CachedDBModel[Label] {
  private lazy val s2Config = new S2CounterConfig(config)
  // enable negative cache
  override val cacheConfig: ConfigurableCacheConfig = new ConfigurableCacheConfig(s2Config.CACHE_MAX_SIZE, s2Config.CACHE_TTL_SECONDS, true)

  val log = LoggerFactory.getLogger(this.getClass)

  def findByName(label: String, useCache: Boolean = true): Option[Label] = {
    val sql = SQL("""
                    |select * from labels where label = ?
                  """.stripMargin).bind(label).map { rs => Label(rs) }
    if (useCache) {
      cache.withCache(s"$label") {
        sql.single().apply()
      }
    } else {
      sql.single().apply()
    }
  }
}

object Label {
  def apply(rs: WrappedResultSet): Label = {
    Label(rs.int("id"), rs.string("label"),
      rs.int("src_service_id"), rs.string("src_column_name"), rs.string("src_column_type"),
      rs.int("tgt_service_id"), rs.string("tgt_column_name"), rs.string("tgt_column_type"),
      rs.boolean("is_directed"), rs.string("service_name"), rs.int("service_id"), rs.string("consistency_level"),
      rs.string("hbase_table_name"), rs.intOpt("hbase_table_ttl"), rs.boolean("is_async"))
  }
}

case class Label(id: Int, label: String,
                 srcServiceId: Int, srcColumnName: String, srcColumnType: String,
                 tgtServiceId: Int, tgtColumnName: String, tgtColumnType: String,
                 isDirected: Boolean = true, serviceName: String, serviceId: Int, consistencyLevel: String = "strong", hTableName: String, hTableTTL: Option[Int], isAsync: Boolean = false) {
}
