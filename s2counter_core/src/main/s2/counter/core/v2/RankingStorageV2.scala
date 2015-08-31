package s2.counter.core.v2

import aa.mango.json._
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import s2.counter.core.RankingCounter._
import s2.counter.core.{RankingKey, RankingResult, RankingStorage}
import s2.models.Counter

import scalaj.http.Http

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 22..
 */
class RankingStorageV2(config: Config) extends RankingStorage {
  private[counter] val log = LoggerFactory.getLogger(this.getClass)
  val s2graphUrl = config.getString("s2graph.url")
  private val K_MAX = 500
  private val SERVICE_NAME = "s2counter"
  private val COLUMN_NAME = "bucket"
  private val LABEL_NAME = "s2counter_ranking"
  private val DOUBLE_PRECISION = 10000d

  override def getTopK(key: RankingKey, k: Int): Option[RankingResult] = {
    val offset = 0
    val limit = k
    val bucket = makeBucket(key)

    val values = getEdges(bucket, offset, limit)
//    println(values)
    Some(RankingResult(0d, values))

  }

  override def update(key: RankingKey, value: RankingValueMap, k: Int): Unit = {
    val bucket = makeBucket(key) // srcVertex
    val currentRankingMap: Map[String, Double] = getEdges(bucket, 0, k).toMap

    val newRankingSeq = (currentRankingMap ++ value.mapValues(_.score)).toSeq.sortBy(-_._2)

    if (newRankingSeq.length > k) {
      delete(key, newRankingSeq.drop(k).toMap)
    }

    insertOrUpdateEdges(bucket, newRankingSeq.take(k).toMap)
  }

  override def update(values: Seq[(RankingKey, RankingValueMap, Int)]): Unit = {
    for {
      (key, value, k) <- values
    } {
      update(key, value, k)
    }
  }

  def delete(key: RankingKey, deleteRankingMap: Map[String, Double]) = {
    // /graphs/edges/delete
    val bucket = makeBucket(key)

    val timestamp: Long = System.currentTimeMillis
    val valueArray = deleteRankingMap.map { case (item, value) =>
      S2GraphInsertUpdateDeleteReq(bucket, item, "s2counter_ranking", timestamp, Map("score" -> value.toLong))
    }.toArray

    Http(s"$s2graphUrl/graphs/edges/delete")
      .postData(toJson(valueArray))
      .header("content-type", "application/json").execute()
  }

  override def delete(key: RankingKey): Unit = {
    // /graphs/edges/delete
    val bucket = makeBucket(key)
    val offset = 0
    val limit = K_MAX
    val values = getEdges(bucket, offset, limit)

    val timestamp: Long = System.currentTimeMillis
    val valueArray = values.map { case (item, value) =>
      S2GraphInsertUpdateDeleteReq(bucket, item, "s2counter_ranking", timestamp, Map("score" -> value.toLong))
    }.toArray

    Http(s"$s2graphUrl/graphs/edges/delete")
      .postData(toJson(valueArray))
      .header("content-type", "application/json").execute()
  }

  private def getEdges(bucket: String, offset: Int, limit: Int): Seq[(String, Double)] = {
    // bucket 으로 가상의 source vertex 생성하고
    // itemId 가 target vertex
    // s2counter_top_k 라는 label 에
    // indexedProp 에 score 추가
    // score 대로 sorting 된 결과 ...

    val srcVertices = Array(S2GraphGetEdgesReqSrcVertex(SERVICE_NAME, COLUMN_NAME, bucket))
    val steps = Array(S2GraphGetEdgesReqStep(LABEL_NAME, "out", offset, limit, Map("score" -> 1)))
    val request = S2GraphGetEdgesReq(srcVertices, Array(steps))

    //    val postFuture = Post.json(s"$s2graphUrl/graphs/getEdges", request)

    val response = Http(s"$s2graphUrl/graphs/getEdges")
      .postData(toJson(request))
      .header("content-type", "application/json").asString

//    println(toJson(request))

//    println("getEdges response code : " + response.code)
    response.body match {
      case x if x.isEmpty =>
        Nil
      case body =>
        for {
          result <- fromJson[S2GraphGetEdgesRes](response.body).results
        } yield {
          val item = result.to
          val score = result.props.score / DOUBLE_PRECISION
          (item, score)
        }
    }
  }

  private def insertOrUpdateEdges(bucket: String, value: Map[String, Double]): Unit = {
    // https://github.daumkakao.com/shon-0/s2graph#1-insert---post-graphsedgesinsert
    // https://github.daumkakao.com/shon-0/s2graph#3-update---post-graphsedgesupdate
    //    [
    //    {"from":1,"to":104,"label":"graph_test","timestamp":1417616433, "props": {"is_hidden":true}},
    //    {"from":1,"to":104,"label":"graph_test","timestamp":1417616434, "props": {"weight":100}}
    //    ]
    val timestamp: Long = System.currentTimeMillis
    val valueArray = value.map { case (item, score) =>
      S2GraphInsertUpdateDeleteReq(bucket, item, "s2counter_ranking", timestamp,
        Map("score" -> (score * DOUBLE_PRECISION).toLong)
      )
    }.toArray

//    println(toJson(valueArray))

    val code = Http(s"$s2graphUrl/graphs/edges/update")
      .postData(toJson(valueArray))
      .header("content-type", "application/json").asString.code
//    println("update response code : " + code)
  }

  override def prepare(policy: Counter, rateActionOpt: Option[String]): Unit = {
    // do nothing
  }

  override def getTopK(keys: Seq[RankingKey], k: Int): Seq[(RankingKey, RankingResult)] = {
    Nil
  }
}

case class S2GraphInsertUpdateDeleteReq(
  from: String,
  to: String,
  label: String,
  timestamp: Long,
  props: Map[String, Long]
  )

case class S2GraphGetEdgesReqSrcVertex(serviceName: String, columnName: String, id: String)

case class S2GraphGetEdgesReqStep(label: String, direction: String, offset: Int, limit: Int, scoring: Map[String, Int])

case class S2GraphGetEdgesReq(
  srcVertices: Array[S2GraphGetEdgesReqSrcVertex],
  steps: Array[Array[S2GraphGetEdgesReqStep]]
  )

case class S2GraphGetEdgesResResultProps(score: Long)

case class S2GraphGetEdgesResResult(to: String, props: S2GraphGetEdgesResResultProps)

case class S2GraphGetEdgesRes(
  results: Array[S2GraphGetEdgesResResult]
  )
