package controllers

import java.util.concurrent.TimeUnit
import com.beachape.metascraper.Messages.{ScrapeUrl, ScrapedData}
import com.beachape.metascraper.Scraper
import com.daumkakao.s2graph.core.{Graph, GraphUtil}
import com.google.common.cache.CacheBuilder
import dispatch.Http
import play.api.libs.json.{Json, JsObject}
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future

/**
 * Created by shon on 9/11/15.
 */
object LikeController extends Controller with RequestParser {

  import ApplicationController._

  val scraper = new Scraper(Http, Seq("http", "https"))

  //  val fData: Future[ScrapedData] = scraper.fetch(ScrapeUrl("https://google.com"))
  val cacheTTL = 60000
  lazy val cache = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheTTL, TimeUnit.MILLISECONDS)
    .maximumSize(10000)
    .build[java.lang.Integer, ScrapedData]()
  val userUrlLabelName = "user_urls"
  val urlSelfLabelName = "url_url"

  def like(user: String, url: String) = withHeaderAsync(parse.anyContent) { request =>
    likeInner(user, url).map { ret =>
      Ok(ret)
    }
  }
  def likeInner(user: String,
                url: String): Future[Boolean] = {
    val ts = System.currentTimeMillis()
    for {
      scrapedData <- toScrapedData(url, ts)
      urlProps = toJsObject(scrapedData)
      urlJson = Json.obj("timestamp" -> ts, "from" -> url,
        "to" -> toShortenUrl(url), "label" -> urlSelfLabelName,
        "props"  -> urlProps)
      edgeJson = Json.obj("timestamp" -> ts, "from" -> user,
        "to" -> url, "label" -> userUrlLabelName, "props" -> urlProps)
      edges = toEdges(Json.arr(edgeJson, urlJson), "insertBulk")
      rets <- Graph.mutateEdges(edges)
    } yield rets.forall(identity)
  }

  private def toShortenUrl(url: String): String = {
    url
  }
  private def toScrapedData(url: String, ts: Long): Future[ScrapedData] = {
    val cacheTsVal = ts / cacheTTL
    val urlHash = GraphUtil.murmur3(cacheTsVal + url)
    val oldVal = cache.getIfPresent(urlHash)
    if (oldVal == null) scraper.fetch(ScrapeUrl(url))
    else {
      Future.successful(oldVal)
    }
  }
  //FIXME:
  private def toJsObject(scrapedData: ScrapedData): JsObject = {
    Json.obj(
      "url" -> scrapedData.url,
      "mainImageUrl" -> scrapedData.mainImageUrl,
      "title" -> scrapedData.title,
      "description" -> scrapedData.description,
      "imageUrls" -> scrapedData.imageUrls.mkString(",")
    )
  }
}
