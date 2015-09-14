package controllers

import java.net.{URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit
import actors.{LikeUtil, QueueActor}
import com.beachape.metascraper.Messages.{ScrapeUrl, ScrapedData}
import com.beachape.metascraper.Scraper
import com.daumkakao.s2graph.core.ExceptionHandler.KafkaMessage
import com.daumkakao.s2graph.core.mysqls.Service
import com.daumkakao.s2graph.core.{ExceptionHandler, Graph, GraphUtil}
import com.daumkakao.s2graph.logger
import com.google.common.cache.CacheBuilder
import config.Config
import dispatch.Http
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.Logger
import play.api.libs.json.{JsValue, Json, JsObject}
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future

/**
 * Created by shon on 9/11/15.
 */
object LikeController extends Controller with RequestParser {
  import scala.concurrent.ExecutionContext.Implicits.global
  import ApplicationController._
  import actors.KafkaConsumerWithThrottle._

  val scraper = new Scraper(Http, Seq("http", "https"))

  def badAccessTokenException(accessToken: String) = new RuntimeException(s"bad accessToken: $accessToken")
  def notAllowedActionTypeException(actionType: String) = new RuntimeException(s"not allowd action type: $actionType")
  val cacheTTL = 60000
  val filter = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheTTL, TimeUnit.MILLISECONDS)
    .maximumSize(10000)
    .build[Integer, String]()
  /** select */
//  def select(accessToken: String, user: String) = withHeaderAsync(parse.anyContent) { request =>
//    val actionType = "like"
//    val service = Service.findByAccessToken(accessToken).getOrElse(throw badAccessTokenException(accessToken))
//    val labelName = LikeUtil.userUrlLabels.get(actionType).getOrElse(throw notAllowedActionTypeException(actionType))
//    val limit = 100
//    val serviceId = service.id.get
//    val queryString = s"""
//          | {"srcVertices": [{"serviceName": "${LikeUtil.serviceName}", "columnName": "${LikeUtil.srcColumnName}", "id": "$user"}],
//          | "steps": [
//          |    {"step": [
//          |      {
//          |        "label": "$labelName", "limit": $limit, "index": "IDX_SERVICE_ID",
//          |        "interval": {"from": {"serviceId": $serviceId}, "to": {"serviceId": $serviceId}}}]
//          |      },
//          |    {"step": [{"label": "${LikeUtil.urlSelfLabelName}", "limit": 1}]}
//          |  ]
//       """.stripMargin
//    val queryJson = Json.parse(queryString)
//    QueryController.getEdgesInner(queryJson)
//  }

  def selectAll(user: String) = withHeaderAsync(parse.anyContent) { request =>
    val actionType = "like"
    val labelName = LikeUtil.userUrlLabels.get(actionType).getOrElse(throw notAllowedActionTypeException(actionType))

    val queryJson =
      Json.parse(s"""
         |{
         |    "srcVertices": [
         |        {
         |            "serviceName": "${LikeUtil.serviceName}",
         |            "columnName": "${LikeUtil.srcColumnName}",
         |            "id": "$user"
         |        }
         |    ],
         |    "steps": [
         |        {
         |            "step": [
         |              {"label": "$labelName", "limit": 100, "index": "_PK"}
         |            ]
         |        },
         |        {
         |            "step": [
         |              {"label": "${LikeUtil.urlSelfLabelName}", "limit": 1}
         |            ]
         |        }
         |    ]
         |}
       """.stripMargin)
    QueryController.getEdgesInner(queryJson)
  }

  /** write */

  def scrape(rawUrl: String) = withHeaderAsync(parse.anyContent) { request =>
//    val url = urlWithProtocol(URLDecoder.decode(encodedUrl, "utf-8"))
    val url = urlWithProtocol(rawUrl)
    for {
      scrapedData <- scraper.fetch(ScrapeUrl(url))
      edge = toUrlSelfEdge(url, toShortenUrl(url), scrapedData)
      ret <- Graph.mutateEdgeWithWait(edge)
    } yield {
      val json = Json.obj("url" -> url, "data" -> toJsObject(scrapedData))
      jsonResponse(json)
    }
  }

  def like(accessToken: String) = withHeaderAsync(parse.json) { request =>
    val service = Service.findByAccessToken(accessToken).getOrElse(throw badAccessTokenException(accessToken))
    likeInner(service.id.get)(request.body).map { ret =>
      Ok(s"$ret")
    }
  }

  def likes(accessToken: String) = withHeaderAsync(parse.json) { request =>
    val service = Service.findByAccessToken(accessToken).getOrElse(throw badAccessTokenException(accessToken))

    val jsVals = request.body.asOpt[Seq[JsValue]].getOrElse(Nil)
    likesInner(service.id.get)(jsVals).map { rets =>
      Ok(s"$rets")
    }
  }

  def likesInner(serviceId: Int)(jsVals: Seq[JsValue]): Future[Seq[Boolean]] = {
    val futures = jsVals.map(likeInner(serviceId)(_))
    Future.sequence(futures)
  }

  def likeInner(serviceId: Int)(jsVal: JsValue): Future[Boolean] = {
    val (user, url, actionType) = toParams(jsVal)
    likeInner(serviceId, user, url, actionType)
  }
  /** fire user-url action edge into local queue, and fire scrap request simultaneously */
  def likeInner(serviceId: Int, user: String,
                url: String, actionType: String): Future[Boolean] = {
    val labelName = LikeUtil.userUrlLabels.get(actionType).getOrElse(throw notAllowedActionTypeException(actionType))
    val ts = System.currentTimeMillis()
    val edgeJson = Json.obj("timestamp" -> ts, "from" -> user, "to" -> url,
      "label" -> labelName, "props" -> Json.obj("serviceId" -> serviceId))
    val edge = toEdge(edgeJson, "insert")

    // fire insert request for edge for (user, url)
    Logger.info(s"$edge")
    QueueActor.router ! edge
    logger.info(s"$QueueActor.scrapeRouter")
    ExceptionHandler.enqueue(KafkaMessage(new ProducerRecord[String, String](Config.KAFKA_SCRAPE_TOPIC, null, url)))
//
//    UrlScrapeActor.router ! url
    Future.successful(true)
  }


  /** helper for manupulation */
  def toParams(jsValue: JsValue) = {
    val user = (jsValue \ "user").asOpt[String].getOrElse(throw new RuntimeException("user is not provided."))
    val url = (jsValue \ "url").asOpt[String].getOrElse(throw new RuntimeException("url is not provided."))
    val actionType = (jsValue \ "actionType").asOpt[String].getOrElse("like")
    (user, url, actionType)
  }

}
