package org.apache.s2graph.loader

import java.sql.{Driver, DriverManager}

import com.kakao.s2graph.core.Graph
import org.apache.s2graph.lambda.source.KafkaInput
import org.apache.s2graph.lambda.{Params, Data, EmptyData, BaseDataProcessor}
import scala.collection.JavaConversions._

case class StatParams(phase: String, dbUrl: String, brokerList: String) extends Params

class Stat(params: StatParams) extends BaseDataProcessor[KafkaInput, EmptyData](params) {

  override protected def processBlock(input: KafkaInput): EmptyData = {
    val phase = params.phase
    val dbUrl = params.dbUrl
    val brokerList = params.brokerList

    val elements = input.rdd.mapPartitions { partition =>
      GraphSubscriberHelper.apply(phase, dbUrl, "none", brokerList)
      partition.map { case (key, msg) =>
        Graph.toGraphElement(msg) match {
          case Some(elem) =>
            val serviceName = elem.serviceName
            msg.split("\t", 7) match {
              case Array(_, operation, log_type, _, _, label, _*) =>
                Seq(serviceName, label, operation, log_type).mkString("\t")
              case _ =>
                Seq("no_service_name", "no_label", "no_operation", "parsing_error").mkString("\t")
            }
          case None =>
            Seq("no_service_name", "no_label", "no_operation", "no_element_error").mkString("\t")
        }
      }
    }

    logger.info(s"${input.time} ${elements.count()}")

    Data.emptyData
  }
}
