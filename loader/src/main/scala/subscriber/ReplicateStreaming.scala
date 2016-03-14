package subscriber

import com.kakao.s2graph.core.GraphUtil
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}
import s2.spark.{HashMapParam, SparkApp}
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.streaming.Durations._
import scala.collection.mutable.{HashMap => MutableHashMap}

object ReplicateStreaming extends SparkApp {
  lazy val className = getClass.getName.stripSuffix("$")
  val config = ConfigFactory.load()

  val inputTopics = Set(config.getString("TOPIC"))
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "etl_to_counter")
  val kafkaParam = Map(
    "group.id" -> groupId,
    "metadata.broker.list" -> config.getString("kafka.metadata.broker.list"),
    "zookeeper.connect" -> config.getString("kafka.zookeeper"),
    "zookeeper.connection.timeout.ms" -> "10000"
  )
  val streamHelper = StreamHelper(kafkaParam)

  override def run(): Unit = {
    validateArgument("interval")
    val (intervalInSec) = seconds(args(0).toLong)

    val conf = sparkConf(s"$strInputTopics: $className")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val acc = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    /**
      * read message from etl topic and join user profile from graph and then produce whole message to counter topic
      */
    val stream = streamHelper.createStream[String, String, StringDecoder, StringDecoder](ssc, inputTopics)

    // etl logic
    stream.foreachRDD { (rdd, ts) =>
      val nextRdd = {
        rdd.repartition(sc.defaultParallelism).foreachPartition { part =>
          // convert to edge format
          val items = {
            for {
              (k, v) <- part
              line <- GraphUtil.parseString(v)
            } yield {
              acc += ("Edges", 1)
              line
            }
          }

        }
        rdd
      }
      streamHelper.commitConsumerOffsets(nextRdd.asInstanceOf[HasOffsetRanges])
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
