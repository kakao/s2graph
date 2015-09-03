package s2.models

import java.util.Calendar

import com.daumkakao.s2graph.core.mysqls.ServiceColumn
import org.apache.http.HttpStatus
import org.slf4j.LoggerFactory
import play.api.libs.json._
import s2.config.StreamingConfig
import s2.util.{CollectionCache, CollectionCacheConfig, Retry}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 1. 19..
 */
case class DimensionProps(service: String, column: String, props: JsValue)

object DimensionPropsConfig {
  val cacheConfig = CollectionCacheConfig(StreamingConfig.PROFILE_CACHE_MAX_SIZE, StreamingConfig.PROFILE_CACHE_TTL_SECONDS,
    negativeCache = true, 3600) // negative ttl 1 hour
}

object DimensionProps extends CollectionCache[Option[DimensionProps]](DimensionPropsConfig.cacheConfig) {
  // using play-ws without play app
  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val client = new play.api.libs.ws.ning.NingWSClient(builder.build)
  private val log = LoggerFactory.getLogger(this.getClass)

  private val retryCnt = 3

  def findByIdFromPAI(column: ServiceColumn, id: String): DimensionProps = {
    val serviceName = column.service.serviceName
    val columnName = column.columnName

    val cacheKey = s"$serviceName.$columnName.$id"

    withCache(cacheKey) {
      val props: JsValue = {
        if (serviceName == "kakao" && columnName == "account_id") {
          // lookup PAI
          // sample: http://beta-pai.daumkakao.io/profile/user/18579615?service_code=s2
          val url = s"http://beta-pai.daumkakao.io/profile/user/$id?service_code=s2"

          // play-ws
          val resp = Await.result(client.url(url).get(), 1 seconds)

          resp.status match {
            case HttpStatus.SC_OK =>
              val content = Json.parse(resp.body)
              content \ "user"
            case HttpStatus.SC_NOT_FOUND => // Empty dimension
              Json.obj()
            case _ => // ERROR exception
              log.error(s"url: $url, err: ${resp.status}, msg: ${resp.body}")
              Json.obj()
          }
        }
        else {
          Json.obj()
        }
      }
      Some(DimensionProps(serviceName, columnName, props))
    }.get
  }

  val vertexToLabel = Map(
    ("kakao", "account_id") -> ("_s2graph_account_id_talk_user_id", "out"),
    ("kakao", "service_user_id") -> ("_s2graph_service_user_id_talk_user_id", "out"),
    ("kakaostory", "profile_id") -> ("_s2graph_profile_id_talk_user_id", "out"),
    ("kakaotalk", "talk_user_id") -> ("_s2graph_account_id_talk_user_id", "in")
  )

  private[models] def rawQueryToGraph(column: ServiceColumn, ids: List[String]): Option[JsValue] = {
    val serviceName = column.service.serviceName
    val columnName = column.columnName

    makeQuery(column, ids).flatMap { query =>
      log.debug(s"$query")

      val jsonQuery = Json.parse(query)
      val url = StreamingConfig.GRAPH_URL

      try {
        Retry(retryCnt, withSleep = false) {
          val resp = Await.result(client.url(url).post(jsonQuery), 1 seconds)

          resp.status match {
            case HttpStatus.SC_OK =>
              Some(Json.parse(resp.body))
            case _ =>
              log.error(s"$serviceName.$columnName.${ids.mkString(",")} is error(${resp.status}). msg: ${resp.body}")
              None
          }
        }
      } catch {
        case e: Exception =>
          // error logging 후 None 으로 일단 보낸다.
          log.error(s"${e.getMessage} ids: ${ids.mkString(",")}")
          None
      }
    }
  }
  
  private[models] def makeQuery(column: ServiceColumn, ids: List[String]): Option[String] = {
    val serviceName = column.service.serviceName
    val columnName = column.columnName

    lazy val vertices = s"""{"serviceName": "$serviceName", "columnName": "$columnName", "ids": [${ids.mkString(",")}]}"""

    vertexToLabel.get((serviceName, columnName)).map { case (label, direction) =>
      s"""
         |{
         |    "srcVertices": [
         |        $vertices
         |    ],
         |    "steps": [
         |        [
         |            {
         |                "label": "$label",
         |                "direction": "$direction",
         |                "offset": 0,
         |                "maxAttempt": 10,
         |                "rpcTimeout": 1000,
         |                "limit": 1
         |            }
         |        ]
         |    ]
         |}
      """.stripMargin
    }
  }

  private[models] def queryToGraph(column: ServiceColumn, ids: List[String])(rawQuery: (ServiceColumn, List[String]) => Option[JsValue]): Map[String, JsValue] = {
    lazy val curYear = Calendar.getInstance().get(Calendar.YEAR)
    lazy val columnName = column.columnName

    val propSeq = for {
      jsonBody <- rawQuery(column, ids).toSeq
      rst <- (jsonBody \ "results").as[JsArray].value
    } yield {
      val fromId = rst \ "from"
      val toId = rst \ "to"
      val props = Seq((columnName, fromId), ("talk_user_id", toId)) ++ (for {
        pKey <- Seq("birth_year", "gender", "living_area")
        pValue <- (rst \ "props" \ pKey).asOpt[JsValue]
      } yield {
        pKey match {
          case "birth_year" =>
            val birthYear = pValue.as[Int]
            Seq(
              (pKey, JsString(birthYear.toString)),
              ("age", JsString((curYear - birthYear).toString))
            )
          case _ => Seq((pKey, pValue))
        }
      }).flatten

      (fromId match {
        case JsNumber(v) => v.toString()
        case JsString(v) => v
        case _ => throw new RuntimeException(s"from must be a number or a string. ${fromId.getClass}")
      }, Json.toJson(props.toMap))
    }
    propSeq.toMap
  }

  def findByIdFromGraph(column: ServiceColumn, id: String): DimensionProps = {
    val serviceName = column.service.serviceName
    val columnName = column.columnName

    val cacheKey = s"$serviceName.$columnName.$id"

    withCache(cacheKey) {
      log.debug(s"cache miss: $localHostname, $cacheKey")
      queryToGraph(column, List(id))(rawQueryToGraph).get(id).map { props =>
        DimensionProps(serviceName, columnName, props)
      }
    }.getOrElse(DimensionProps(serviceName, columnName, Json.obj()))
  }

  private def makeCacheKey(column: ServiceColumn, id: String): String = {
    val serviceName = column.service.serviceName
    val columnName = column.columnName
    s"$serviceName.$columnName.$id"
  }

  def cachePreFetch(column: ServiceColumn, ids: List[String]) = {
    val serviceName = column.service.serviceName
    val columnName = column.columnName

    // filter non cached item
    val nonCachedIds = ids.filterNot(id => contains(makeCacheKey(column, id)))
    log.debug(s"column: $column, nonCachedIds: $nonCachedIds")
    val fetched = queryToGraph(column, nonCachedIds)(rawQueryToGraph)

    for {
      id <- ids
    } yield {
      val cacheKey = makeCacheKey(column, id)
      withCache(cacheKey) {
        fetched.get(id).map(props => DimensionProps(serviceName, columnName, props))
      }
    }
  }
}
