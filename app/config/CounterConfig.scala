package config

import play.api.Play

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 9. 3..
 */
object CounterConfig {
  lazy val conf = Play.current.configuration
  // kafka
  lazy val KAFKA_TOPIC_COUNTER = conf.getString("kafka.topic.counter").getOrElse("s2counter-alpha")
  lazy val KAFKA_TOPIC_COUNTER_TRX = conf.getString("kafka.topic.counter-trx").getOrElse("s2counter-trx-alpha")
}
