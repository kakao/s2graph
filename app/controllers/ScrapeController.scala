package controllers

import java.net.URL
import java.util.concurrent.{TimeUnit, Executors}

import actors.KafkaConsumerWithThrottle
import com.beachape.metascraper.extractors.SchemaFactory
import com.beachape.metascraper.extractors.html.{HtmlSchema, NormalPage, OpenGraph, HtmlSchemas}
import com.daumkakao.s2graph.core.Graph
import com.daumkakao.s2graph.logger
import com.google.common.cache.CacheBuilder
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig, Response}
import dispatch.{url, StatusCode, Http}
import org.apache.commons.validator.routines.UrlValidator
import org.joda.time.Seconds
import org.jsoup.nodes.Document
import play.api.libs.json.Json
import play.api.mvc.Controller


import scala.concurrent.duration.Duration
import scala.concurrent.duration.Duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.util.hashing.MurmurHash3
import com.beachape.metascraper.Messages.{ScrapeUrl, ScrapedData}
import com.beachape.metascraper.Scraper
import com.beachape.metascraper.StringOps._

/**
 * Created by shon on 9/23/15.
 */
object ScrapeController extends Controller with RequestParser {

  import scala.concurrent.ExecutionContext.Implicits.global
  import ApplicationController._
  import actors.KafkaConsumerWithThrottle._

  private val httpExecutorThreads: Int = 10
  private val maxConnectionsPerHost: Int = 30
  private val connectionTimeoutInMs: Int = 10000
  private val requestTimeoutInMs: Int = 15000
  private val validSchemas = Seq("http", "https")
  // Http client config
  private val followRedirects = true
  private val connectionPooling = true
  private val compressionEnabled = true

  private val executorService = Executors.newFixedThreadPool(httpExecutorThreads)
  private val config = new AsyncHttpClientConfig.Builder()
    .setExecutorService(executorService)
    .setIOThreadMultiplier(1) // otherwise we might not have enough threads
    .setMaximumConnectionsPerHost(maxConnectionsPerHost)
    .setAllowPoolingConnection(connectionPooling)
    .setAllowSslConnectionPool(connectionPooling)
    .setConnectionTimeoutInMs(connectionTimeoutInMs)
    .setRequestTimeoutInMs(requestTimeoutInMs)
    .setCompressionEnabled(compressionEnabled)
    .setFollowRedirects(followRedirects).build
  private val asyncHttpClient = new AsyncHttpClient(config)
  private val httpClient = new Http(asyncHttpClient)

  val scraper = new Scraper(httpClient, Seq("http", "https"))
  val defaultSchemaFactories = Seq(HtmlSchemas(OpenGraph, NormalPage))
  Await.result(scraper.fetch(ScrapeUrl("http://www.google.com")), Duration(10, TimeUnit.SECONDS))
  //
  case class MyScraper(httpClient: Http, urlSchemas: Seq[String])(implicit ex: ExecutionContext)
    extends Scraper(httpClient, urlSchemas)(ex) {

    private val urlValidator = new UrlValidator(urlSchemas.toArray)

    override def fetch(message: ScrapeUrl): Future[ScrapedData] = {
      val messageUrl = message.url
      if (!urlValidator.isValid(messageUrl))
        Future.failed(new IllegalArgumentException(s"Invalid url ${message.url}"))
      else if (messageUrl.hasImageExtension) {
        Future.successful(ScrapedData(messageUrl, messageUrl, messageUrl, messageUrl, Seq(messageUrl)))
      } else {
        val host = new URL(messageUrl).getHost
        val requestHeaders = Map(
          "Host" -> Seq(host),
          "User-Agent" -> Seq(message.userAgent),
          "Accept" -> Seq("*/*"))
        val request = url(messageUrl).setHeaders(requestHeaders)
        logger.error(s"${messageUrl}, ${requestHeaders}")
        val resp = httpClient(request)
        resp map (s => extractData(s, messageUrl, message.schemaFactories, message.numberOfImages))
      }
    }

    override def extractData(resp: Response, url: String, schemaFactories: Seq[SchemaFactory], numberOfImages: Int): ScrapedData = {
      if (resp.getStatusCode / 100 == 2) {
        val schemas = schemaFactories.toStream.flatMap(f => Try(f.apply(resp)).getOrElse(Nil)) // Stream in case we have expensive factories
        val maybeUrl = schemas.flatMap(s => Try(s.extractUrl).toOption).find(_.isDefined).getOrElse(None)
        val maybeTitle = schemas.flatMap(s => Try(s.extractTitle).toOption).find(_.isDefined).getOrElse(None)
        val maybeDescription = schemas.flatMap(s => Try(s.extractDescription).toOption).find(_.isDefined).getOrElse(None)
        val imageUrls = schemas.foldLeft(Stream.empty[String]) { (acc, schema) => acc ++ Try(schema.extractImages).getOrElse(Nil) }
        val maybeMainImg = schemas.flatMap(s => Try(s.extractMainImage).toOption).find(_.isDefined).getOrElse(None)
        ScrapedData(
          url = maybeUrl.getOrElse(url),
          title = maybeTitle.getOrElse(""),
          description = maybeDescription.getOrElse(""),
          imageUrls = imageUrls.take(numberOfImages),
          mainImageUrl = maybeMainImg.getOrElse("")
        )
      } else {
        logger.error(s"${resp.getStatusCode}, ${resp.getHeaders}")
        throw StatusCode(resp.getStatusCode)
      }
    }
  }

  def scrape(rawUrl: String) = withHeaderAsync(parse.anyContent) { request =>
    //    val url = urlWithProtocol(URLDecoder.decode(encodedUrl, "utf-8"))
    val url = urlWithProtocol(rawUrl)
    //    val url = rawUrl
    import KafkaConsumerWithThrottle._
    val hashKey = MurmurHash3.stringHash(url)
    val oldVal = KafkaConsumerWithThrottle.filter.getIfPresent(hashKey)
    val scrapeUrl = ScrapeUrl(url)
    val future = if (oldVal == null) {
      for {
        scrapedData <- scraper.fetch(scrapeUrl)
        edge = toUrlSelfEdge(url, toShortenUrl(url), scrapedData)
        ret <- Graph.mutateEdgeWithWait(edge)
      } yield {
        KafkaConsumerWithThrottle.filter.put(hashKey, scrapedData)
        scrapedData
      }
    } else {
      Future.successful(oldVal)
    }
    future.map { data =>
      val json = Json.obj("url" -> url, "data" -> toJsObject(data, serializeArray = false))
      jsonResponse(json, "Access-Control-Allow-Origin" -> "*",
        "Access-Control-Allow-Methods" -> "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers" -> "Content-Type, X-Requested-With, Accept",
        // cache access control response for one day
        "Access-Control-Max-Age" -> (60 * 60 * 24).toString)
    }

  }
}
