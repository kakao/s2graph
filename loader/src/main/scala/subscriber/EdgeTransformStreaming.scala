package subscriber

import com.kakao.s2graph.core.GraphConfig
import com.kakao.s2graph.core.mysqls.Model
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import org.apache.spark.streaming.kafka.StreamHelper
import s2.config.S2ConfigFactory
import s2.spark.{HashMapParam, SparkApp}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.ExecutionContext

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

  val inputTopics = Set(graphConfig.KAFKA_LOG_TOPIC)
  val strInputTopics = inputTopics.mkString(",")
  val groupId = buildKafkaGroupId(strInputTopics, "etl_to_counter")
  val kafkaParam = Map(
    "group.id" -> groupId,
    "metadata.broker.list" -> graphConfig.KAFKA_METADATA_BROKER_LIST,
    "zookeeper.connect" -> graphConfig.KAFKA_ZOOKEEPER,
    "zookeeper.connection.timeout.ms" -> "10000"
  )
  val streamHelper = StreamHelper(kafkaParam)

  // should implement in derived class
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
      rdd.foreachPartitionWithOffsetRange { case (osr, part) =>
        assert(initialize)

//        // convert to edge format
//        val items = {
//          for {
//            (k, v) <- part
//            line <- GraphUtil.parseString(v)
//            item <- CounterEtlFunctions.parseEdgeFormat(line)
//          } yield {
//            acc += ("Edges", 1)
//            item
//          }
//        }
//
//        // join user profile
//        val joinItems = items.toList.groupBy { e =>
//          (e.service, e.action)
//        }.flatMap { case ((service, action), v) =>
//          CounterEtlFunctions.checkPolicyAndMergeDimension(service, action, v)
//        }
//
//        // group by kafka partition key and send to kafka
//        val m = MutableHashMap.empty[Int, mutable.MutableList[CounterEtlItem]]
//        joinItems.foreach { item =>
//          if (item.useProfile) {
//            acc += ("ETL", 1)
//          }
//          val k = getPartKey(item.item, 20)
//          val values: mutable.MutableList[CounterEtlItem] = m.getOrElse(k, mutable.MutableList.empty[CounterEtlItem])
//          values += item
//          m.update(k, values)
//        }
//        m.foreach { case (k, v) =>
//          v.map(_.toKafkaMessage).grouped(1000).foreach { grouped =>
//            acc += ("Produce", grouped.size)
//            producer.send(new KeyedMessage[String, String](StreamingConfig.KAFKA_TOPIC_COUNTER, null, k, grouped.mkString("\n")))
//          }
//        }

        streamHelper.commitConsumerOffset(osr)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
