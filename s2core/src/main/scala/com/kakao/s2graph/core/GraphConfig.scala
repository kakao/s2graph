package com.kakao.s2graph.core

import com.kakao.s2graph.core.utils.Configuration._
import com.typesafe.config.Config

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
class GraphConfig(conf: Config) {
  // HBASE
  lazy val HBASE_ZOOKEEPER_QUORUM = conf.getOrElse("hbase.zookeeper.quorum", "localhost")

  // HBASE CLIENT
  lazy val ASYNC_HBASE_CLIENT_FLUSH_INTERVAL = conf.getOrElse("async.hbase.client.flush.interval", 1000).toShort
  lazy val RPC_TIMEOUT = conf.getOrElse("hbase.client.operation.timeout", 1000)
  lazy val MAX_ATTEMPT = conf.getOrElse("hbase.client.operation.maxAttempt", 3)

  // PHASE
  lazy val PHASE = conf.getOrElse("phase", "dev")

  // CACHE
  lazy val CACHE_TTL_SECONDS = conf.getOrElse("cache.ttl.seconds", 600)
  lazy val CACHE_MAX_SIZE = conf.getOrElse("cache.max.size", 10000)

  //KAFKA
  lazy val KAFKA_METADATA_BROKER_LIST = conf.getOrElse("kafka.metadata.broker.list", "localhost")
  lazy val KAFKA_PRODUCER_POOL_SIZE = conf.getOrElse("kafka.producer.pool.size", 0)
  lazy val KAFKA_ZOOKEEPER = conf.getOrElse("kafka.zookeeper", "localhost")
  lazy val KAFKA_LOG_TOPIC = s"s2graphIn$PHASE"
  lazy val KAFKA_LOG_TOPIC_ASYNC = s"s2graphIn${PHASE}Async"
  lazy val KAFKA_FAIL_TOPIC = s"s2graphIn${PHASE}Failed"

  // is query or write
  lazy val IS_QUERY_SERVER = conf.getOrElse("is.query.server", true)
  lazy val IS_WRITE_SERVER = conf.getOrElse("is.write.server", true)

  // query limit per step
  lazy val QUERY_HARD_LIMIT = conf.getOrElse("query.hard.limit", 300)

  // local queue actor
  lazy val LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE = conf.getOrElse("local.queue.actor.max.queue.size", 10000)
  lazy val LOCAL_QUEUE_ACTOR_RATE_LIMIT = conf.getOrElse("local.queue.actor.rate.limit", 1000)
}
