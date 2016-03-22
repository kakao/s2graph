package subscriber

import com.kakao.s2graph.core.GraphUtil
import com.typesafe.config.ConfigFactory
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.spark.Accumulable
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, StreamHelper}
import play.api.libs.json.JsArray
import s2.spark.{WithKafka, HashMapParam, SparkApp}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}

object ReplicateStreaming extends SparkApp with WithKafka {
  lazy val className = getClass.getName.stripSuffix("$")
  val config = ConfigFactory.load()

  val inputTopics = Set(config.getString("kafka.topic.graph"), config.getString("kafka.topic.graph-async"))
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "replicate")
  val brokerList = config.getString("kafka.metadata.broker.list")
  val kafkaParam = Map(
    "group.id" -> groupId,
    "metadata.broker.list" -> brokerList,
    "zookeeper.connect" -> config.getString("kafka.zookeeper"),
    "zookeeper.connection.timeout.ms" -> "10000"
  )
  val streamHelper = StreamHelper(kafkaParam)

  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val client = new play.api.libs.ws.ning.NingWSClient(builder.build)

  val apiPath = config.getString("s2graph.api-path")
  val batchSize = config.getInt("s2graph.batch-size")
  lazy val isFailRetryer = if ( config.hasPath("s2graph.fail-try") ) config.getBoolean("s2graph.fail-try") else false
  lazy val isReplicator = !isFailRetryer

  lazy val failedTopic = if ( config.hasPath("kafka.topic.failed") ) config.getString("kafka.topic.failed") else ""
  lazy val failedProducer = getProducer[String, String](brokerList)

  def toKeyedMessage(lines: Seq[String]) = lines.map{ line => new KeyedMessage[String, String](failedTopic, line) }

  def postProcess(lines: Seq[String]): Unit = {
    if ( isFailRetryer ) {
      // throw exception to retry mini-batch
      throw new RuntimeException(s"Failed queue retry response failed : \n${lines.mkString("\n")}")
    } else {
      // publish `lines` to failed topic
      failedProducer.send(toKeyedMessage(lines): _*)
    }
  }

  def sendToGraph(lines: Seq[String], acc: Accumulable[MutableHashMap[String, Long], (String, Long)]): Unit = {
    val startTs = System.currentTimeMillis()
    val future = client.url(apiPath).post(lines.mkString("\n"))
    try {
      val response = Await.result(future, 1 minute)
      val respJson = response.json.as[JsArray]
      val isSuccess = respJson.value.map(_.as[Boolean]).forall(identity)

      if (!isSuccess) {
        postProcess(lines)
        acc += "Fail" -> lines.length
      } else {
        acc += "Success" -> lines.length
      }
    } catch {
      case e: Exception =>
        logError(s"Fail processing \n${lines.mkString("\n")}", e)
        acc += "Exception" -> lines.length
        throw e
    }
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
          val fn =
            // TODO check `isFailRetryer` accessibility in this scope
            if ( isFailRetryer ) ReplicateFunctions.parseRetryLog  _
            else ReplicateFunctions.parseReplicationLog _

          // convert to element
          val items = for {
            (k, v) <- part
            line <- GraphUtil.parseString(v)
            replLog <- fn(line)
          } yield replLog

          // send to graph
          for {
            grouped <- items.grouped(batchSize)
          } {
            sendToGraph(grouped, acc)
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
