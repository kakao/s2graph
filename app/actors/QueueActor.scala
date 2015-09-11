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

object Protocol {

  case object Flush

  case object FlushAll

}
object LikeUtil {
  val serviceName = "kakao_test"
  val srcColumnName = "ad_id"
  val urlSelfLabelName = "kakao_shorten_url_test"
  val allowedActionTypes = Set("like", "share", "click")
  val userUrlLabels = Map("like" -> "kakao_like_test", "share" -> "kakao_share_test", "click" -> "kakao_click_test")
}

object QueueActor {
  /** we are throttling down here so fixed number of actor to constant */
  var router: ActorRef = _

  def init() = {
    router = Akka.system.actorOf(Props[QueueActor])
  }

  def shutdown() = {
    router ! FlushAll

    Akka.system.shutdown()
    Thread.sleep(Config.ASYNC_HBASE_CLIENT_FLUSH_INTERVAL * 2)
  }
}

class QueueActor extends Actor with ActorLogging {
  logger.error(s"$this")
  import Protocol._

  implicit val ec = context.system.dispatcher
  //  logger.error(s"QueueActor: $self")
  val queue = mutable.Queue.empty[GraphElement]
  var queueSize = 0L
  val maxQueueSize = Config.LOCAL_QUEUE_ACTOR_MAX_QUEUE_SIZE
  val timeUnitInMillis = 10
  val rateLimitTimeStep = 1000 / timeUnitInMillis
  val rateLimit = Config.LOCAL_QUEUE_ACTOR_RATE_LIMIT / rateLimitTimeStep


  context.system.scheduler.schedule(Duration.Zero, Duration(timeUnitInMillis, TimeUnit.MILLISECONDS), self, Flush)

  override def receive: Receive = {
    case element: GraphElement =>

      if (queueSize > maxQueueSize) {
        ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_FAIL_TOPIC, element, None))
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
      Graph.mutateElements(elementsToFlush)

      if (flushSize > 0) {
        logger.info(s"flush: $flushSize, $queueSize, $this")
      }

    case FlushAll =>
      Graph.mutateElements(queue)
      context.stop(self)

    case _ => logger.error("unknown protocol")
  }
}