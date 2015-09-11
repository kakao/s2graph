package actors

import java.util.concurrent.TimeUnit

import actors.Protocol.{FlushAll}
import akka.pattern.ask
import akka.actor._
import akka.routing.{SmallestMailboxRouter, RoundRobinRoutingLogic}
import akka.util.Timeout
import com.beachape.metascraper.Messages.{ScrapeUrl, ScrapedData}
import com.beachape.metascraper.ScraperActor
import com.daumkakao.s2graph.core.ExceptionHandler._
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.logger

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

object UrlScrapeActor {
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

}
class UrlScrapeActor extends Actor with RequestParser {
  logger.error(s"$this")

  import Protocol._

  implicit val ec = context.system.dispatcher
  //  logger.error(s"QueueActor: $self")
  val queue = scala.collection.mutable.Queue.empty[String]
  var queueSize = 0L
  val maxQueueSize = Config.LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE
  val timeUnitInMillis = 10
  val rateLimitTimeStep = 1000 / timeUnitInMillis
  val rateLimit = Config.LOCAL_QUEUE_ACTOR_RATE_LIMIT / rateLimitTimeStep

  val scraper = context.system.actorOf(ScraperActor())
  val urlSelfLabelName = LikeUtil.urlSelfLabelName

  val cacheTTL = 60000
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
  private def urlWithProtocol(url: String): String = {
    if (url.startsWith("http://")) url
    else if (url.startsWith("https://")) url
    else {
      s"http://$url"
    }
  }
  private def toShortenUrl(url: String): String = {
    url
  }
  private def toJsObject(scrapedData: ScrapedData): JsObject = {
    Json.obj(
      "url" -> scrapedData.url,
      "mainImageUrl" -> scrapedData.mainImageUrl,
      "title" -> scrapedData.title,
      "description" -> scrapedData.description,
      "imageUrls" -> scrapedData.imageUrls.mkString(",")
    )
  }
  private def work(urlToScrape: String): Unit = {
    Logger.info(s"scrape url: $urlToScrape")
    val url = urlWithProtocol(urlToScrape)
    val urlHash = GraphUtil.murmur3(url)
    val oldVal = cache.getIfPresent(urlHash)
    if (oldVal == null) {
      for {
        future <- scraper.ask(ScrapeUrl(url)).mapTo[Either[Throwable,ScrapedData]]
      } {
        future match {
          case Left(throwable) => {
            Logger.error(s"failed to scrape url: $urlToScrape")
            //TODO: publish to failed queue.
          }
          case Right(data) => {
            val ts = System.currentTimeMillis()
            val urlEdge = toEdge(Json.obj("timestamp" -> ts,
              "from" -> urlToScrape, "to" -> toShortenUrl(urlToScrape), "label" -> urlSelfLabelName,
              "props" -> toJsObject(data)), "insert")
            Graph.mutateEdge(urlEdge)
            Logger.debug(s"new url: $url is updated.")
          }
        }
      }

    } else {
      Logger.debug(s"cache hit: $url")
    }
  }
}