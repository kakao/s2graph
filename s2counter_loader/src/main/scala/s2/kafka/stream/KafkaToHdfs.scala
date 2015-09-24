package s2.kafka.stream

import java.text.SimpleDateFormat

import com.daumkakao.s2graph.core.GraphUtil
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.kafka.KafkaRDDFunctions.rddToKafkaRDDFunctions
import s2.config.StreamingConfig
import s2.spark.{HashMapParam, SparkApp}
import s2.util.{HdfsUtil, UnitConverter}

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 29..
 */
object KafkaToHdfs extends SparkApp {
  lazy val className = getClass.getName.stripSuffix("$")

  // 상속받은 클래스에서 구현해줘야 하는 함수
  override def run(): Unit = {
    validateArgument("interval", "topic", "path", "zk")
    val (intervalInSec, topic, path, zk) = (seconds(args(0).toLong), args(1), args(2), args(3))

    val groupId = buildKafkaGroupId(topic, "kafka_to_hdfs")
    val kafkaParam = Map(
      "group.id" -> groupId,
      "metadata.broker.list" -> StreamingConfig.KAFKA_BROKERS,
      "zookeeper.connect" -> StreamingConfig.KAFKA_ZOOKEEPER,
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset" -> "smallest"
    )

    val conf = sparkConf(s"$topic: $className")
    val ssc = streamingContext(conf, intervalInSec)
    val sc = ssc.sparkContext

    val acc = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    val stream = getStreamHelper(kafkaParam).createStream[String, String, StringDecoder, StringDecoder](ssc, topic.split(',').toSet)
    stream.foreachRDD { rdd =>
      rdd.foreachPartitionWithOffsetRange { case (osr, part) =>
        val partByDate = {
          for {
            (k, v) <- part
            line <- GraphUtil.parseString(v)
            sp = GraphUtil.split(line)
          } yield {
            val date = new SimpleDateFormat("yyyy/MM/dd/HH").format(UnitConverter.toMillis(sp(0).toLong))
            (date, line)
          }
        }.toSeq.groupBy(_._1).mapValues(_.map(_._2))

        for {
          (date, lines) <- partByDate
          fullPath = s"$path/$date"
        } {
          try {
            HdfsUtil.operationHa(fullPath, zk, lines)(HdfsUtil.appendToTextFile)
            acc += ("Append OK", lines.length)
          } catch {
            case ex: Throwable =>
              acc += ("Append Failed", lines.length)
          }
        }
        getStreamHelper(kafkaParam).commitConsumerOffset(osr)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
