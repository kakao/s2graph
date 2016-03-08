package s2.config

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 4. 7..
 */
object StreamingConfig extends ConfigFunctions(S2ConfigFactory.config) {
  // kafka
  val KAFKA_ZOOKEEPER = getOrElse("kafka.zookeeper", "localhost")
  val KAFKA_BROKERS = getOrElse("kafka.brokers", "localhost")
  val KAFKA_TOPIC_GRAPH = getOrElse("kafka.topic.graph", "s2graphInalpha")
  val KAFKA_TOPIC_GRAPH_ASYNC = getOrElse("kafka.topic.graph-async", "s2graphInalphaAsync")
  val KAFKA_TOPIC_ETL = getOrElse("kafka.topic.etl", "s2counter-etl-alpha")
  val KAFKA_TOPIC_COUNTER = getOrElse("kafka.topic.counter", "s2counter-alpha")
  val KAFKA_TOPIC_COUNTER_TRX = getOrElse("kafka.topic.counter-trx", "s2counter-trx-alpha")
  val KAFKA_TOPIC_COUNTER_FAIL = getOrElse("kafka.topic.counter-fail", "s2counter-fail-alpha")

  // dimension
  val DIM_CACHE_TTL_SECONDS = getOrElse("dim.cache.ttl.seconds", 60 * 60 * 24)    // default 1 day
  val DIM_CACHE_MAX_SIZE = getOrElse("dim.cache.max.size", 10000)
  val DIM_REQ_BATCH_SIZE = getOrElse("dim.req.batch.size", 100)

  // graph url
  val GRAPH_URL = getOrElse("s2graph.url", "")
  val GRAPH_READONLY_URL = getOrElse("s2graph.read-only.url", GRAPH_URL)
}
