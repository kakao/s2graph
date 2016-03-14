package subscriber

import com.kakao.s2graph.core.GraphUtil
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}
import play.api.libs.json.{JsObject, Json}
import s2.spark.{HashMapParam, SparkApp}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.Await
import scala.concurrent.duration._

object ReplicateStreaming extends SparkApp {
  lazy val className = getClass.getName.stripSuffix("$")
  val config = ConfigFactory.load()

  val inputTopics = Set(config.getString("kafka.topic.graph"), config.getString("kafka.topic.graph-async"))
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "replicate")
  val kafkaParam = Map(
    "group.id" -> groupId,
    "metadata.broker.list" -> config.getString("kafka.metadata.broker.list"),
    "zookeeper.connect" -> config.getString("kafka.zookeeper"),
    "zookeeper.connection.timeout.ms" -> "10000"
  )
  val streamHelper = StreamHelper(kafkaParam)

  def parseReplicationLog(line: String): Option[String] = {
    val sp = GraphUtil.split(line)
    val prop = if (sp.length > 6) sp(6) else "{}"
    val jsProps = Json.parse(prop)
    (jsProps \ "_rep_").asOpt[Boolean] match {
      case Some(b) =>
        if (b) {
          // drop
          None
        } else {
          // replicate
          logError(s"Invalid replication flag.")
          None
        }
      case None =>
        // replicate
        val newProps = jsProps.as[JsObject] ++ Json.obj("_rep_" -> true)
        sp(6) = newProps.toString()
        Option(sp.mkString("\t"))
    }
  }

  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val client = new play.api.libs.ws.ning.NingWSClient(builder.build)

  val apiPath = config.getString("s2graph.api-path")
  val batchSize = config.getInt("s2graph.batch-size")

  def sendToGraph(lines: Seq[String]): Unit = {
    val startTs = System.currentTimeMillis()
    val future = client.url(apiPath).post(lines.mkString("\n"))
    Await.result(future, 1 minute)
    val elapsedTime = System.currentTimeMillis() - startTs

    if (elapsedTime > 300) {
      logWarning(s"Slow mutate. elapsedTime: $elapsedTime, requestSize: ${lines.length}")
    }
  }

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
          // convert to element
          /*
          1457927050825	insert	e	kshp1wKyK25A_160209155438392	96	toros_1boon_article_personal	{"score":0.0,"meta_key":"56c2e02ca2b8815f0863903d"}
           */
          val items = for {
            (k, v) <- part
            line <- GraphUtil.parseString(v)
            replLog <- parseReplicationLog(line)
          } yield replLog

          // send to graph
          for {
            grouped <- items.grouped(batchSize)
          } {
            sendToGraph(grouped)
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
