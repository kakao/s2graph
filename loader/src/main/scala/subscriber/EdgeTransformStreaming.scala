package subscriber

import com.kakao.s2graph.EdgeTransform
import com.kakao.s2graph.client.RestClient
import com.kakao.s2graph.core.mysqls.Model
import com.kakao.s2graph.core.utils.Configuration._
import com.kakao.s2graph.core.{Graph, GraphConfig, GraphUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.HasOffsetRanges
import s2.config.S2ConfigFactory
import s2.spark.{HashMapParam, SparkApp}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
object EdgeTransformStreaming extends SparkApp {
  lazy val config = S2ConfigFactory.config
  lazy val className = getClass.getName.stripSuffix("$")

  implicit val graphEx = ExecutionContext.Implicits.global

  val initialize = {
    println("streaming initialize")
    Model(config)
    true
  }

  val graphConfig = new GraphConfig(config)

  val inputTopics = Set(graphConfig.KAFKA_LOG_TOPIC, graphConfig.KAFKA_LOG_TOPIC_ASYNC)
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "etl_to_graph")
  val kafkaParam = Map(
    "group.id" -> groupId,
    "metadata.broker.list" -> graphConfig.KAFKA_METADATA_BROKER_LIST,
    "zookeeper.connect" -> graphConfig.KAFKA_ZOOKEEPER,
    "zookeeper.connection.timeout.ms" -> "10000"
  )

  val graphUrl = config.getOrElse("s2graph.url", "http://localhost")
  val graphReadOnlyUrl = config.getOrElse("s2graph.read-only.url", graphUrl)
  lazy val rest = RestClient(graphUrl)
  lazy val readOnlyRest = RestClient(graphReadOnlyUrl)
  lazy val transformer = new EdgeTransform(rest, readOnlyRest)

  lazy val streamHelper = getStreamHelper(kafkaParam)

  // should implement in derived class
  override def run(): Unit = {
    validateArgument("interval", "clear")
    val (interval, clear) = (args(0).toLong, args(1).toBoolean)
    if (clear) {
      streamHelper.kafkaHelper.consumerGroupCleanup()
    }
    val intervalInSec = seconds(interval)

    val conf = sparkConf(s"$strInputTopics: $className")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val acc = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    val stream = streamHelper.createStream[String, String, StringDecoder, StringDecoder](ssc, inputTopics)

    stream.foreachRDD { (rdd, ts) =>
      val nextRdd = {
        rdd.foreachPartition { part =>
          assert(initialize)

          // convert to edge format
          val orgEdges = for {
            (k, v) <- part
            line <- GraphUtil.parseString(v)
            maybeEdge <- Try {
              Graph.toEdge(line)
            }.toOption
            edge <- maybeEdge
          } yield edge

          // transform and send edges to graph
          for {
            orgEdgesGrouped <- orgEdges.grouped(10)
            transEdges = transformer.transformEdges(orgEdgesGrouped)
            rets = transformer.loadEdges(transEdges, withWait = true)
          } {
            acc += ("Input", orgEdgesGrouped.length)
            acc += ("Transform", rets.count(x => x))
            transEdges.zip(rets).filterNot(_._2).foreach { case (e, _) =>
              logError(s"failed to loadEdge: ${e.toLogString}")
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
