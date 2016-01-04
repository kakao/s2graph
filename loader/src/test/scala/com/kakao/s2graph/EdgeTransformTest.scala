package com.kakao.s2graph

import com.kakao.s2graph.client.{BulkRequest, GetEdgesRequest, GraphRestClient}
import com.kakao.s2graph.core.mysqls.{Etl, Model}
import com.kakao.s2graph.core.{Graph, GraphConfig, Management}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsString, Json}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 22..
  */
class EdgeTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val graphEx = ExecutionContext.Implicits.global

  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val client = new play.api.libs.ws.ning.NingWSClient(builder.build)

  lazy val config = ConfigFactory.load()
  val graphConfig = new GraphConfig(config)
  Model(config)

  lazy val _s2graph = new Graph(config)
  lazy val _rest = new GraphRestClient(client, config.getString("s2graph.url"))
  lazy val _edgeTransform = new EdgeTransform(_rest)

  val logger = LoggerFactory.getLogger(getClass)

  override def beforeAll: Unit = {
    if (Management.findService("test").isEmpty) {
      Management.createService("test", graphConfig.HBASE_ZOOKEEPER_QUORUM, "test", 1, None, "gz")
    }

    if (Management.findLabel("test1").isEmpty) {
      Management.createLabel("test1", "test", "c1", "string", "test", "c1", "string", true, "test", Nil, Nil, "weak", None, None, isAsync = false)
    }
    val l1Id = Management.findLabel("test1").get.id.get
    if (Management.findLabel("test2").isEmpty) {
      Management.createLabel("test2", "test", "c1", "string", "test", "c1", "string", true, "test", Nil, Nil, "weak", None, None, isAsync = false)
    }
    val l2Id = Management.findLabel("test2").get.id.get
    Etl.create(l1Id.toInt, l2Id.toInt)
  }

  override def afterAll: Unit = {
    Etl.findByOriginalLabelIds(Management.findLabel("test1").get.id.get).foreach { etl =>
      Etl.delete(etl.id.get)
    }
    Management.deleteLabel("test2")
    Management.deleteLabel("test1")
  }

  "EdgeTransform" should "extract string target vertex id" in {
    val json = Json.obj(
      "results" -> Json.arr(
        Json.obj(
          "to" -> "1"
        )
      )
    )

    val result = _edgeTransform.extractTargetVertex[String](json)
    result.isSuccess should equal(true)
    result.get should equal("1")
  }

  it should "extract numeric target vertex id" in {
    val json = Json.obj(
      "results" -> Json.arr(
        Json.obj(
          "to" -> 1
        )
      )
    )

    val result = _edgeTransform.extractTargetVertex[Int](json)
    result.isSuccess should equal(true)
    result.get should equal(1)
  }

  it should "extract props" in {
    val json = Json.obj(
      "results" -> Json.arr(
        Json.obj(
          "to" -> 1,
          "props" -> Json.obj(
            "p1" -> "1"
          )
        )
      )
    )

    val result = _edgeTransform.extractProps(json)
    result.isSuccess should equal(true)
    (result.get \ "p1") should equal(JsString("1"))
  }

  it should "transform edge" in {
    val e1 = Management.toEdge(1, "insert", "1", "0", "test1", "out", "{}")
    val future = _edgeTransform.transformEdge(e1)

    val result = Await.result(future, 10 seconds)
    result should not be empty

    val e2 = result.get
    e2.label.label should equal("test2")

    val rets = Await.result(_s2graph.mutateEdges(Seq(e2), withWait = true), 10 seconds)
    rets should not be empty

    rets.head should equal(true)
  }

  it should "transform edge with rest api" in {
    val e1 = Management.toEdge(1, "insert", "2", "0", "test1", "out", "{}")
    val future = _edgeTransform.transformEdge(e1)

    val result = Await.result(future, 10 seconds)
    result should not be empty

    val e2 = result.get
    e2.label.label should equal("test2")

    val rets = Await.result(_edgeTransform.loadEdges(Seq(e2), withWait = true), 10 seconds)
    rets should not be empty

    rets.head should equal(true)

    val request = GetEdgesRequest(Json.obj(
      "srcVertices" -> Json.arr(
        Json.obj(
          "serviceName" -> "test",
          "columnName" -> "c1",
          "id" -> "2"
        )
      ),
      "steps" -> Json.arr(
        Json.obj(
          "step" -> Json.arr(
            Json.obj(
              "label" -> "test2"
            )
          )
        )
      )
    ))
    val value = Await.result(
      for {
        ret2 <- _rest.post(request).map(_.json)
      } yield ret2,
      10 seconds
    )

    logger.info(s"$value")

    _edgeTransform.extractTargetVertex[String](value).get should equal("0")
  }
}
