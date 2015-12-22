package subscriber

import com.kakao.s2graph.EdgeTransform
import com.kakao.s2graph.core.mysqls.Model
import com.kakao.s2graph.core.rest.RestCaller
import com.kakao.s2graph.core.{Graph, GraphConfig, GraphUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import s2.config.S2ConfigFactory
import s2.spark.{HashMapParam, SparkApp}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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

  lazy val _s2graph = new Graph(config)
  lazy val _rest = new RestCaller(_s2graph)
  lazy val _edgeTransform = new EdgeTransform(_rest)

  // should implement in derived class
  override def run(): Unit = {
    validateArgument("interval")
    val interval = args(0).toLong
    val intervalInSec = seconds(interval)

    val conf = sparkConf(s"$strInputTopics: $className")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val acc = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    val stream = getStreamHelper(kafkaParam).createStream[String, String, StringDecoder, StringDecoder](ssc, inputTopics)

    stream.foreachRDD { (rdd, ts) =>
      rdd.foreachPartitionWithOffsetRange { case (osr, part) =>
        assert(initialize)

        // convert to edge format
        val orgEdges = for {
          (k, v) <- part
          line <- GraphUtil.parseString(v)
          edge <- Graph.toEdge(line)
        } yield edge

        // transform and send edges to graph
        val future = Future.sequence {
          for {
            orgEdgesGrouped <- orgEdges.grouped(10)
          } yield {
            acc += ("Input", orgEdgesGrouped.length)
            for {
              transEdges <- _edgeTransform.changeEdges(orgEdgesGrouped)
              rets <- _s2graph.mutateEdges(transEdges, withWait = true)
            } yield {
              acc += ("Transform", rets.count(x => x))
              transEdges.zip(rets).filterNot(_._2).foreach { case (e, _) =>
                logError(s"failed to mutateEdge: ${e.toLogString}")
              }
            }
          }
        }

        Await.result(future, interval seconds)

        getStreamHelper(kafkaParam).commitConsumerOffset(osr)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
