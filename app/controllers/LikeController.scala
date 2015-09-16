package controllers

import java.net.{URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit
import actors.{KafkaConsumerWithThrottle, LikeUtil, QueueActor}
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
import scala.util.hashing.MurmurHash3

object LikeController extends Controller with RequestParser {

  import scala.concurrent.ExecutionContext.Implicits.global
  import ApplicationController._
  import actors.KafkaConsumerWithThrottle._

  val scraper = new Scraper(Http, Seq("http", "https"))

  def badAccessTokenException(accessToken: String) = new RuntimeException(s"bad accessToken: $accessToken")

  def notAllowedActionTypeException(actionType: String) = new RuntimeException(s"not allowd action type: $actionType")

  val cacheTTL = 5000
  val filter = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheTTL, TimeUnit.MILLISECONDS)
    .maximumSize(10000)
    .build[Integer, String]()

  /** select */
  def select(accessToken: String, user: String, actionType: String) = withHeaderAsync(parse.anyContent) { request =>
    val service = Service.findByAccessToken(accessToken).getOrElse(throw badAccessTokenException(accessToken))
    val labelName = LikeUtil.userUrlLabels.get(actionType).getOrElse(throw notAllowedActionTypeException(actionType))
    val serviceId = service.id.get
    val queryJson = Json.obj("srcVertices" -> Json.arr(Json.obj("serviceName" -> LikeUtil.serviceName, "columnName" -> LikeUtil.srcColumnName, "id" -> user)),
      "steps" -> Json.arr(
        Json.obj("step" -> Json.arr(Json.obj("label" -> labelName, "index" -> "IDX_SERVICE_ID",
          "interval" -> Json.obj("from" -> Json.obj("serviceId" -> serviceId), "to" -> Json.obj("serviceId" -> serviceId))))),
        Json.obj("step" -> Json.arr(Json.obj("label" -> LikeUtil.urlSelfLabelName, "limit" -> 1)))
      )
    )
    logger.debug(s"$queryJson")
    QueryController.getEdgesInner(queryJson)
  }

  def selectAll(user: String, actionType: String) = withHeaderAsync(parse.anyContent) { request =>
    val labelName = LikeUtil.userUrlLabels.get(actionType).getOrElse(throw notAllowedActionTypeException(actionType))

    val queryJson =
      Json.obj("srcVertices" -> Json.arr(Json.obj("serviceName" -> LikeUtil.serviceName, "columnName" -> LikeUtil.srcColumnName, "id" -> user)),
        "steps" -> Json.arr(
          Json.obj("step" -> Json.arr(Json.obj("label" -> labelName, "index" -> "_PK"))),
          Json.obj("step" -> Json.arr(Json.obj("label" -> LikeUtil.urlSelfLabelName, "limit" -> 1, "cacheTTL" -> cacheTTL)))
        )
      )
    logger.debug(s"$queryJson")
    QueryController.getEdgesInner(queryJson)
  }

  /** write */

  def scrape(rawUrl: String) = withHeaderAsync(parse.anyContent) { request =>
    //    val url = urlWithProtocol(URLDecoder.decode(encodedUrl, "utf-8"))
    val url = urlWithProtocol(rawUrl)
    import KafkaConsumerWithThrottle._
    val hashKey = MurmurHash3.stringHash(url)
    val oldVal = KafkaConsumerWithThrottle.filter.getIfPresent(hashKey)
    if (oldVal == null) {
      for {
        scrapedData <- scraper.fetch(ScrapeUrl(url))
        edge = toUrlSelfEdge(url, toShortenUrl(url), scrapedData)
        ret <- Graph.mutateEdgeWithWait(edge)
      } yield {
        KafkaConsumerWithThrottle.filter.put(hashKey, scrapedData)
        val json = Json.obj("url" -> url, "data" -> toJsObject(scrapedData))
        jsonResponse(json)
      }
    } else {
      Future.successful(jsonResponse(toJsObject(oldVal)))
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
