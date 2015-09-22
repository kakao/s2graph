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

object SocialController extends Controller with RequestParser {

  import scala.concurrent.ExecutionContext.Implicits.global
  import ApplicationController._
  import actors.KafkaConsumerWithThrottle._

  val scraper = new Scraper(Http, Seq("http", "https"))

  def badAccessTokenException(accessToken: String) = new RuntimeException(s"bad accessToken: $accessToken")

  def notAllowedActionTypeException(actionType: String) = new RuntimeException(s"not allowd action type: $actionType")



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
        val json = Json.obj("url" -> url, "data" -> toJsObject(scrapedData, serializeArray = false))
        jsonResponse(json, "Access-Control-Allow-Origin" -> "*",
          "Access-Control-Allow-Methods" -> "GET, POST, PUT, DELETE, OPTIONS",
          "Access-Control-Allow-Headers" -> "Content-Type, X-Requested-With, Accept",
          // cache access control response for one day
          "Access-Control-Max-Age" -> (60 * 60 * 24).toString)
      }
    } else {
      Future.successful(jsonResponse(toJsObject(oldVal)))
    }
  }

  def like(accessToken: String) = withHeaderAsync(parse.json) { request =>
    val service = Service.findByAccessToken(accessToken).getOrElse(throw badAccessTokenException(accessToken))
    likeInner(service)(request.body).map { ret =>
      Ok(s"$ret")
    }
  }

  def likes(accessToken: String) = withHeaderAsync(parse.json) { request =>
    val service = Service.findByAccessToken(accessToken).getOrElse(throw badAccessTokenException(accessToken))

    val jsVals = request.body.asOpt[Seq[JsValue]].getOrElse(Nil)
    likesInner(service)(jsVals).map { rets =>
      Ok(s"$rets")
    }
  }

  def likesInner(service: Service)(jsVals: Seq[JsValue]): Future[Seq[Boolean]] = {
    val futures = jsVals.map(likeInner(service)(_))
    Future.sequence(futures)
  }

  def likeInner(service: Service)(jsVal: JsValue): Future[Boolean] = {
    val (user, url, actionType) = toParams(jsVal)
    likeInner(service, user, url, actionType)
  }

  /** fire user-url action edge into local queue, and fire scrap request simultaneously */
  def likeInner(service: Service, user: String,
                url: String, actionType: String): Future[Boolean] = {

    val labelName = LikeUtil.userUrlLabels.get(actionType).getOrElse(throw notAllowedActionTypeException(actionType))
    val ts = System.currentTimeMillis()
    val edgeJson = Json.obj("timestamp" -> ts, "from" -> user, "to" -> url,
      "label" -> labelName, "props" -> Json.obj("serviceName" -> service.serviceName))

    // publish url to scrape kafka topic so background workers can consume.
    ExceptionHandler.enqueue(KafkaMessage(new ProducerRecord[String, String](Config.KAFKA_SCRAPE_TOPIC, null, url)))

    for {
      insertedRet <- EdgeController.tryMutates(edgeJson, "insert")
    } yield {
      true
    }
  }


  /** helper for manupulation */
  def toParams(jsValue: JsValue) = {
    val user = (jsValue \ "user").asOpt[String].getOrElse(throw new RuntimeException("user is not provided."))
    val url = (jsValue \ "url").asOpt[String].getOrElse(throw new RuntimeException("url is not provided."))
    val actionType = (jsValue \ "actionType").asOpt[String].getOrElse("like")
    (user, url, actionType)
  }

}
