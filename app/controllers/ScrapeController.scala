package controllers

import actors.KafkaConsumerWithThrottle
import com.daumkakao.s2graph.core.Graph
import com.google.common.cache.CacheBuilder
import dispatch.Http
import play.api.libs.json.Json
import play.api.mvc.Controller


import scala.concurrent.Future
import scala.util.hashing.MurmurHash3
import com.beachape.metascraper.Messages.{ScrapeUrl, ScrapedData}
import com.beachape.metascraper.Scraper

/**
 * Created by shon on 9/23/15.
 */
object ScrapeController extends Controller with RequestParser {

  import scala.concurrent.ExecutionContext.Implicits.global
  import ApplicationController._
  import actors.KafkaConsumerWithThrottle._

  val scraper = new Scraper(Http, Seq("http", "https"))

  def scrape(rawUrl: String) = withHeaderAsync(parse.anyContent) { request =>
    //    val url = urlWithProtocol(URLDecoder.decode(encodedUrl, "utf-8"))
    val url = urlWithProtocol(rawUrl)

    import KafkaConsumerWithThrottle._
    val hashKey = MurmurHash3.stringHash(url)
    val oldVal = KafkaConsumerWithThrottle.filter.getIfPresent(hashKey)

    val data = if (oldVal == null) {
      for {
        scrapedData <- scraper.fetch(ScrapeUrl(url))
        edge = toUrlSelfEdge(url, toShortenUrl(url), scrapedData)
        ret <- Graph.mutateEdgeWithWait(edge)
      } yield {
        KafkaConsumerWithThrottle.filter.put(hashKey, scrapedData)
        scrapedData
      }
    } else {
      oldVal
    }

    val json = Json.obj("url" -> url, "data" -> toJsObject(data, serializeArray = false))
    Future.successful(jsonResponse(json, "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers" -> "Content-Type, X-Requested-With, Accept",
      // cache access control response for one day
      "Access-Control-Max-Age" -> (60 * 60 * 24).toString))
  }
}
