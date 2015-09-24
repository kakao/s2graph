package s2.counter

import com.daumkakao.s2graph.core.mysqls.{Service, ServiceColumn}
import com.daumkakao.s2graph.core.{Edge, Graph, GraphUtil}
import org.apache.spark.Logging
import play.api.libs.json.{JsObject, Json}
import s2.config.{S2ConfigFactory, StreamingConfig}
import s2.models.{CounterModel, DimensionProps}

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 3. 17..
 */
object CounterETLFunctions extends Logging {
  lazy val filterOps = Seq("insert", "insertBulk", "update", "increment").map(op => GraphUtil.operations(op))
  lazy val preFetchSize = StreamingConfig.PROFILE_PREFETCH_SIZE
  lazy val accountMap = Map("profile_id" -> "kakaostory", "talk_user_id" -> "kakaotalk", "account_id" -> "kakao", "service_user_id" -> "kakao")
  lazy val config = S2ConfigFactory.config
  lazy val counterModel = new CounterModel(config)

  def logToEdge(line: String): Option[Edge] = {
    for {
      elem <- Graph.toGraphElement(line) if elem.isInstanceOf[Edge]
      edge <- Some(elem.asInstanceOf[Edge]).filter { x =>
        filterOps.contains(x.op)
      }
    } yield {
      edge
    }
  }

  def parseEdgeFormat(line: String): Option[CounterETLItem] = {
    /**
     * 1427082276804	insert	edge	19073318	52453027_93524145648511699	story_user_ch_doc_view	{"doc_type" : "l", "channel_subscribing" : "y", "view_from" : "feed"}
     */
    for {
      elem <- Graph.toGraphElement(line) if elem.isInstanceOf[Edge]
      edge <- Some(elem.asInstanceOf[Edge]).filter { x =>
        filterOps.contains(x.op)
      }
    } yield {
      val label = edge.label
      val labelName = label.label
      val tgtService = label.tgtColumn.service.serviceName
      val tgtId = edge.tgtVertex.innerId.toString()
      val srcId = edge.srcVertex.innerId.toString()
      val srcColumnName = edge.srcVertex.serviceColumn.columnName

      // if no exist edge property make empty property
      val dimension = Json.parse(Some(GraphUtil.split(line)).filter(_.length >= 7).map(_(6)).getOrElse("{}"))
      val property = Json.toJson(Map("userId" -> srcId, "userIdType" -> srcColumnName, "value" -> "1"))
      CounterETLItem(edge.ts, tgtService, labelName, tgtId, dimension, property)
    }
  }

  def parseEdgeFormat(lines: List[String]): List[CounterETLItem] = {
    for {
      line <- lines
      item <- parseEdgeFormat(line)
    } yield {
      item
    }
  }
  
  def checkPolicyAndJoinUserProfile(service: String, action: String, items: List[CounterETLItem]): List[CounterETLItem] = {
    counterModel.findByServiceAction(service, action).map { policy =>
      policy.useProfile match {
        case true =>
          joinUserProfile(items)
        case _ =>
          items
      }
    }.getOrElse(List.empty[CounterETLItem])
  }

  def getServiceColumn(serviceName: String, columnName: String): Option[ServiceColumn] = {
    Service.findByName(serviceName).flatMap(_.id).flatMap { id =>
      ServiceColumn.find(id, columnName)
    }
  }

  def joinUserProfile(items: List[CounterETLItem]): List[CounterETLItem] = {
    // all items has same user id type
    items.headOption.flatMap { item =>
      getServiceColumn(accountMap(item.userIdType), item.userIdType)
    }.map { serviceColumn =>
      val ids = items.map { item =>
        item.userId
      }

      // pre-fetch to cache
      ids.grouped(preFetchSize).foreach { group =>
        DimensionProps.cachePreFetch(serviceColumn, group)
      }

      for {
        item <- items
      } yield {
        val srcProps = DimensionProps.findByIdFromGraph(serviceColumn, item.userId).props
        val newDimension = item.dimension.as[JsObject] ++ srcProps.as[JsObject]
        CounterETLItem(item.ts, item.service, item.action, item.item, newDimension, item.property, true)
      }
    }.getOrElse(List.empty[CounterETLItem])
  }
}
