package s2.config

import com.typesafe.config.Config

/**
 * Created by alec on 15. 3. 10..
 */
case class ActorConfig(config: Config) extends ConfigFunctions(config) {
  lazy val AGGREGATE_FLUSH_INTERVAL = getOrElse("actor.aggregate.flush.interval", 1000)
  lazy val AGGREGATE_FLUSH_COUNT = getOrElse("actor.aggregate.flush.count", 1000)

  lazy val KAFKA_POOL_SIZE = getOrElse("actor.kafka.pool.size", 5)
  lazy val KAFKA_BROKERS = config.getString("actor.kafka.brokers")
}
