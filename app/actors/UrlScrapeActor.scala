package actors

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import actors.Protocol.{Consume, Flush, FlushAll}
import akka.pattern.ask
import akka.actor._
import akka.routing.{Broadcast, SmallestMailboxRouter, RoundRobinRoutingLogic}
import akka.util.Timeout
import com.beachape.metascraper.Messages.{ScrapeUrl, ScrapedData}
import com.beachape.metascraper.{Scraper, ScraperActor}
import com.daumkakao.s2graph.core.ExceptionHandler._
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.logger
import com.ning.http.client.{ProxyServer, AsyncHttpClient, AsyncHttpClientConfig}
import dispatch.Http
import kafka.consumer.{Whitelist, ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector

import scala.concurrent.Future
import scala.util.hashing.MurmurHash3

//import com.daumkakao.s2graph.Logger
import com.google.common.cache.{Cache, CacheBuilder}
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
import scala.collection.JavaConversions._
//
//object UrlScrapeActor extends RequestParser {
//  /** we are throttling down here so fixed number of actor to constant */
//  var router: ActorRef = _
//
//  def init() = {
//    router = Akka.system.actorOf(Props[UrlScrapeActor])
//  }
//
//  def shutdown() = {
//    router ! FlushAll
//
//    Akka.system.shutdown()
//    Thread.sleep(Config.ASYNC_HBASE_CLIENT_FLUSH_INTERVAL * 2)
//  }
//  def urlWithProtocol(url: String): String = {
//    if (url.startsWith("http://")) url
//    else if (url.startsWith("https://")) url
//    else {
//      s"http://$url"
//    }
//  }
//  def toShortenUrl(url: String): String = {
//    url
//  }
//  def toJsObject(scrapedData: ScrapedData): JsObject = {
//    Json.obj(
//      "url" -> scrapedData.url,
//      "mainImageUrl" -> scrapedData.mainImageUrl,
//      "title" -> scrapedData.title,
//      "description" -> scrapedData.description,
//      "imageUrls" -> scrapedData.imageUrls.mkString(",")
//    )
//  }
//  def toUrlSelfEdge(url: String, shortenUrl: String, scrapedData: ScrapedData): Edge = {
//    val ts = System.currentTimeMillis()
//    toEdge(Json.obj("timestamp" -> ts,
//      "from" -> url, "to" -> toShortenUrl(url), "label" -> LikeUtil.urlSelfLabelName,
//      "props" -> toJsObject(scrapedData)), "insert")
//  }
//}
//class UrlScrapeActor extends Actor with RequestParser {
//
//  import Protocol._
//  import UrlScrapeActor._
//  implicit val ec = context.system.dispatcher
//  //  logger.error(s"QueueActor: $self")
//  val queue = scala.collection.mutable.Queue.empty[String]
//  var queueSize = 0L
//  val maxQueueSize = Config.LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE
//  val timeUnitInMillis = 10
//  val rateLimitTimeStep = 1000 / timeUnitInMillis
//  val rateLimit = Config.LOCAL_QUEUE_ACTOR_RATE_LIMIT / rateLimitTimeStep
//
////  val scraper = context.system.actorOf(ScraperActor())
//val validSchemas = Seq("http", "https")
//  // Http client config
//  val followRedirects = true
//  val connectionPooling = true
//  val compressionEnabled = true
//
//
//  val httpExecutorThreads: Int = 10
//  val maxConnectionsPerHost: Int = 30
//  val connectionTimeoutInMs: Int = 10000
//  val requestTimeoutInMs: Int = 15000
//
//  private val proxyServer = new ProxyServer("proxy.daumkakao.io", 3128)
//  private val executorService = Executors.newFixedThreadPool(httpExecutorThreads)
//  private val config = new AsyncHttpClientConfig.Builder()
//    .setExecutorService(executorService)
//    .setIOThreadMultiplier(1) // otherwise we might not have enough threads
//    .setMaximumConnectionsPerHost(maxConnectionsPerHost)
//    .setAllowPoolingConnection(connectionPooling)
//    .setAllowSslConnectionPool(connectionPooling)
//    .setConnectionTimeoutInMs(connectionTimeoutInMs)
//    .setRequestTimeoutInMs(requestTimeoutInMs)
//    .setCompressionEnabled(compressionEnabled)
//    .setProxyServer(proxyServer)
//    .setFollowRedirects(followRedirects).build
//  private val asyncHttpClient = new AsyncHttpClient(config)
//  private val httpClient = new Http(asyncHttpClient)
//
//  val scraper = new Scraper(httpClient, validSchemas)
//  val urlSelfLabelName = LikeUtil.urlSelfLabelName
//
//  val cacheTTL = 600000
//  lazy val cache = CacheBuilder.newBuilder()
//    .expireAfterWrite(cacheTTL, TimeUnit.MILLISECONDS)
//    .maximumSize(10000)
//    .build[java.lang.Integer, ScrapedData]()
//  implicit val timeout = Timeout(10 seconds)
//
//  context.system.scheduler.schedule(Duration.Zero, Duration(timeUnitInMillis, TimeUnit.MILLISECONDS), self, Flush)
//
//  override def receive: Receive = {
//    case element: String =>
//      if (queueSize > maxQueueSize) {
//        //        ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_FAIL_TOPIC, element, None))
//        logger.error(s"over flow")
//      } else {
//        queueSize += 1L
//        queue.enqueue(element)
//      }
//
//    case Flush =>
//      val elementsToFlush =
//        if (queue.size < rateLimit) queue.dequeueAll(_ => true)
//        else (0 until rateLimit).map(_ => queue.dequeue())
//
//      val flushSize = elementsToFlush.size
//
//      queueSize -= elementsToFlush.length
//      elementsToFlush.map(e => work(e))
//
//      if (flushSize > 0) {
//        logger.info(s"flush: $flushSize, $queueSize, $this")
//      }
//
//    case FlushAll =>
//      queue.dequeueAll(_ => true).map(work(_))
//      context.stop(self)
//
//    case _ => logger.error("unknown protocol")
//  }
//
//  private def work(urlToScrape: String): Unit = {
//    Logger.info(s"scrape url: $urlToScrape")
//    val url = urlWithProtocol(urlToScrape)
//    val urlHash = GraphUtil.murmur3(url)
//    val oldVal = cache.getIfPresent(urlHash)
//    if (oldVal == null) {
//      Logger.error(s"cache miss: $url, $urlHash")
//      for {
//        future <- scraper.fetch(ScrapeUrl(url)).mapTo[Either[Throwable,ScrapedData]]
////        future <- scraper.ask(ScrapeUrl(url)).mapTo[Either[Throwable,ScrapedData]]
//      } {
//        future match {
//          case Left(throwable) => {
//            Logger.error(s"failed to scrape url: $urlToScrape")
//            //TODO: publish to failed queue.
//          }
//          case Right(data) => {
//            val edge = toUrlSelfEdge(urlToScrape, toShortenUrl(urlToScrape), data)
//            Graph.mutateEdge(edge)
//            Logger.debug(s"new url: $url is updated. $data")
//          }
//        }
//      }
//
//    } else {
//      Logger.error(s"cache hit: $url, $urlHash")
//    }
//  }
//}

object KafkaConsumerWithThrottle extends RequestParser {

  val numOfScraper = 1
  val cacheTTL = 60000
  val rateLimit = 10000




  val topicFilter = new Whitelist(Config.KAFKA_SCRAPE_TOPIC)
  val filter = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheTTL, TimeUnit.MILLISECONDS)
    .maximumSize(10000)
    .build[Integer, ScrapedData]()

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("consumer.timeout.ms", "500")

    new ConsumerConfig(props)
  }
//  def props() = {
//    Props(new KafkaConsumerWithThrottle[String, String](kafkaConsumer, topics, rateLimit, filter, router))
//  }
  /** we are throttling down here so fixed number of actor to constant */
  var workerRouter: ActorRef = _
  var consumerActors: Seq[ActorRef] = _
  var connector: ConsumerConnector = _
  def init() = {
    connector = kafka.consumer.Consumer.createJavaConsumerConnector(
      createConsumerConfig(Config.KAFKA_ZOOKEEPER_QUORUM, "KafkaConsumerWithThrottle"))
    workerRouter = Akka.system.actorOf(Props(new ScrapeWorker(filter)).withRouter(SmallestMailboxRouter(numOfScraper)))
    consumerActors = connector.createMessageStreamsByFilter(topicFilter).map { stream =>
      Akka.system.actorOf(Props(new KafkaConsumerWithThrottle(stream, rateLimit, filter, workerRouter)))
    }
  }
  def shutdown() = {
    workerRouter ! Broadcast(FlushAll)
    for {
      consumerActor <- consumerActors
    } {
      consumerActor ! PoisonPill
    }
    connector.shutdown()
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
  def toJsObject(scrapedData: ScrapedData, serializeArray: Boolean = true): JsObject = {
    Json.obj(
      "url" -> scrapedData.url,
      "mainImageUrl" -> scrapedData.mainImageUrl,
      "title" -> scrapedData.title,
      "description" -> scrapedData.description,
      "imageUrls" -> (if (serializeArray) scrapedData.imageUrls.mkString(",") else scrapedData.imageUrls)
    )
  }
  def toUrlSelfEdge(url: String, shortenUrl: String, scrapedData: ScrapedData): Edge = {
    val ts = System.currentTimeMillis()
    toEdge(Json.obj("timestamp" -> ts,
      "from" -> url, "to" -> toShortenUrl(url), "label" -> LikeUtil.urlSelfLabelName,
      "props" -> toJsObject(scrapedData, serializeArray = true)), "insert")
  }
}

case class KafkaConsumerWithThrottle(kafkaStream: KafkaStream[Array[Byte], Array[Byte]],
                                           rateLimit: Int,
                                           filter: Cache[Integer, ScrapedData],
                                           router: ActorRef) extends Actor with RequestParser {

  implicit val ex = context.system.dispatcher
  var numOfMsgs = 0L
  val timeOutInMillis = 100
  val cacheTTL = 100000

  context.system.scheduler.schedule(Duration.Zero, Duration(1, TimeUnit.SECONDS), self, Consume)

  private def toHashKey(url: String): Int = MurmurHash3.stringHash(url)
  val iter = kafkaStream.iterator()

  override def receive: Actor.Receive = {
    case Consume =>
//      logger.error(s"Consume")
      /** consume kafka message as rateLimit specified */
      try {
        for {
          i <- (0 until rateLimit)
        } {
          val value = new String(iter.next().message())
          logger.error(s"$value")
          val oldVal = filter.getIfPresent(toHashKey(value))
          if (oldVal == null) {
            router ! ScrapeUrl(value)
          } else {
            logger.info(s"$value is cached. ignored")
          }
        }
      } catch {
        case e: kafka.consumer.ConsumerTimeoutException =>
          //logger.error(s"exception", e)
        case e: Exception =>
          logger.error(s"exception", e)
      }
  }
}
class ScrapeWorker(filter: Cache[Integer, ScrapedData]) extends Actor {
  import KafkaConsumerWithThrottle._
  import akka.pattern.ask
  implicit val timeout = Timeout(10 seconds)
  implicit val ex = context.system.dispatcher
  val scraperActor = context.system.actorOf(ScraperActor())

  override def receive: Actor.Receive = {
    case scrapeUrl: ScrapeUrl =>
      for {
        future <- ask(scraperActor, scrapeUrl).mapTo[Either[Throwable,ScrapedData]]
      } {
        future match {
          case Left(throwable) => {
            logger.error(s"failed to scrape url: $scrapeUrl")
            //TODO: publish to failed queue.
          }
          case Right(data) => {
            val edge = toUrlSelfEdge(scrapeUrl.url, toShortenUrl(scrapeUrl.url), data)
            Graph.mutateEdge(edge)
            val hash = MurmurHash3.stringHash(scrapeUrl.url)
            filter.put(hash, data)
            logger.info(s"new url: $scrapeUrl is updated. $data")
          }
        }
      }


  }
}