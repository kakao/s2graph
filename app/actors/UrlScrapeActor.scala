package actors

import java.util.concurrent.{Executors, TimeUnit}

import actors.Protocol.{FlushAll}
import akka.pattern.ask
import akka.actor._
import akka.routing.{SmallestMailboxRouter, RoundRobinRoutingLogic}
import akka.util.Timeout
import com.beachape.metascraper.Messages.{ScrapeUrl, ScrapedData}
import com.beachape.metascraper.{Scraper, ScraperActor}
import com.daumkakao.s2graph.core.ExceptionHandler._
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.logger
import com.ning.http.client.{ProxyServer, AsyncHttpClient, AsyncHttpClientConfig}
import dispatch.Http

import scala.concurrent.Future

//import com.daumkakao.s2graph.Logger
import com.google.common.cache.CacheBuilder
import config.Config
import controllers.RequestParser
import play.api.Logger
import play.api.Play.current
import play.api.libs.concurrent.Akka
import play.api.libs.json.{JsObject, Json}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.language.postfixOps

object UrlScrapeActor extends RequestParser {
  /** we are throttling down here so fixed number of actor to constant */
  var router: ActorRef = _

  def init() = {
    router = Akka.system.actorOf(Props[UrlScrapeActor])
  }

  def shutdown() = {
    router ! FlushAll

    Akka.system.shutdown()
    Thread.sleep(Config.ASYNC_HBASE_CLIENT_FLUSH_INTERVAL * 2)
  }
  def urlWithProtocol(url: String): String = {
    if (url.startsWith("http://")) url
    else if (url.startsWith("https://")) url
    else {
      s"http://$url"
    }
  }
  def toShortenUrl(url: String): String = {
    url
  }
  def toJsObject(scrapedData: ScrapedData): JsObject = {
    Json.obj(
      "url" -> scrapedData.url,
      "mainImageUrl" -> scrapedData.mainImageUrl,
      "title" -> scrapedData.title,
      "description" -> scrapedData.description,
      "imageUrls" -> scrapedData.imageUrls.mkString(",")
    )
  }
  def toUrlSelfEdge(url: String, shortenUrl: String, scrapedData: ScrapedData): Edge = {
    val ts = System.currentTimeMillis()
    toEdge(Json.obj("timestamp" -> ts,
      "from" -> url, "to" -> toShortenUrl(url), "label" -> LikeUtil.urlSelfLabelName,
      "props" -> toJsObject(scrapedData)), "insert")
  }
}
class UrlScrapeActor extends Actor with RequestParser {

  import Protocol._
  import UrlScrapeActor._
  implicit val ec = context.system.dispatcher
  //  logger.error(s"QueueActor: $self")
  val queue = scala.collection.mutable.Queue.empty[String]
  var queueSize = 0L
  val maxQueueSize = Config.LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE
  val timeUnitInMillis = 10
  val rateLimitTimeStep = 1000 / timeUnitInMillis
  val rateLimit = Config.LOCAL_QUEUE_ACTOR_RATE_LIMIT / rateLimitTimeStep

//  val scraper = context.system.actorOf(ScraperActor())
val validSchemas = Seq("http", "https")
  // Http client config
  val followRedirects = true
  val connectionPooling = true
  val compressionEnabled = true


  val httpExecutorThreads: Int = 10
  val maxConnectionsPerHost: Int = 30
  val connectionTimeoutInMs: Int = 10000
  val requestTimeoutInMs: Int = 15000

  private val proxyServer = new ProxyServer("proxy.daumkakao.io", 3128)
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
    .setProxyServer(proxyServer)
    .setFollowRedirects(followRedirects).build
  private val asyncHttpClient = new AsyncHttpClient(config)
  private val httpClient = new Http(asyncHttpClient)

  val scraper = new Scraper(httpClient, validSchemas)
  val urlSelfLabelName = LikeUtil.urlSelfLabelName

  val cacheTTL = 600000
  lazy val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheTTL, TimeUnit.MILLISECONDS)
    .maximumSize(10000)
    .build[java.lang.Integer, ScrapedData]()
  implicit val timeout = Timeout(10 seconds)

  context.system.scheduler.schedule(Duration.Zero, Duration(timeUnitInMillis, TimeUnit.MILLISECONDS), self, Flush)

  override def receive: Receive = {
    case element: String =>
      if (queueSize > maxQueueSize) {
        //        ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_FAIL_TOPIC, element, None))
        logger.error(s"over flow")
      } else {
        queueSize += 1L
        queue.enqueue(element)
      }

    case Flush =>
      val elementsToFlush =
        if (queue.size < rateLimit) queue.dequeueAll(_ => true)
        else (0 until rateLimit).map(_ => queue.dequeue())

      val flushSize = elementsToFlush.size

      queueSize -= elementsToFlush.length
      elementsToFlush.map(e => work(e))

      if (flushSize > 0) {
        logger.info(s"flush: $flushSize, $queueSize, $this")
      }

    case FlushAll =>
      queue.dequeueAll(_ => true).map(work(_))
      context.stop(self)

    case _ => logger.error("unknown protocol")
  }

  private def work(urlToScrape: String): Unit = {
    Logger.info(s"scrape url: $urlToScrape")
    val url = urlWithProtocol(urlToScrape)
    val urlHash = GraphUtil.murmur3(url)
    val oldVal = cache.getIfPresent(urlHash)
    if (oldVal == null) {
      Logger.error(s"cache miss: $url, $urlHash")
      for {
        future <- scraper.fetch(ScrapeUrl(url)).mapTo[Either[Throwable,ScrapedData]]
//        future <- scraper.ask(ScrapeUrl(url)).mapTo[Either[Throwable,ScrapedData]]
      } {
        future match {
          case Left(throwable) => {
            Logger.error(s"failed to scrape url: $urlToScrape")
            //TODO: publish to failed queue.
          }
          case Right(data) => {
            val edge = toUrlSelfEdge(urlToScrape, toShortenUrl(urlToScrape), data)
            Graph.mutateEdge(edge)
            Logger.debug(s"new url: $url is updated. $data")
          }
        }
      }

    } else {
      Logger.error(s"cache hit: $url, $urlHash")
    }
  }
}