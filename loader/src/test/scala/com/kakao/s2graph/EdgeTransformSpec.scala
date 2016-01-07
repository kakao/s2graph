package com.kakao.s2graph

import com.kakao.s2graph.client.{BulkWithWaitRequest, GetEdgesRequest, GraphRestClient}
import com.kakao.s2graph.core.mysqls.{Etl, Model, Service}
import com.kakao.s2graph.core.{Graph, GraphConfig, Management}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsString, Json}
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 22..
  */
class EdgeTransformSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val graphEx = ExecutionContext.Implicits.global

  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val client = new play.api.libs.ws.ning.NingWSClient(builder.build)

  lazy val config = ConfigFactory.load()
  val graphConfig = new GraphConfig(config)
  Model(config)

  val graphUrl = config.getString("s2graph.url")
  lazy val _s2graph = new Graph(config)
  lazy val _rest = new GraphRestClient(client, graphUrl)
  lazy val _edgeTransform = new EdgeTransform(_rest)

  val logger = LoggerFactory.getLogger(getClass)

  def makeBucket(service: Service): Int = {
    implicit val dbSession = AutoSession

    val expId = sql"""INSERT INTO experiments (service_id, service_name, name, description, experiment_type, total_modular) VALUES (
          ${service.id.get},
          ${service.serviceName},
          "test_exp",
          "",
          "u",
          100
         )""".updateAndReturnGeneratedKey().apply()
    val body =
      """
        |{
        |	"srcVertices": [{
        |		"serviceName": "test",
        |		"columnName": "c1",
        |		"id": "#uuid"
        |	}],
        |	"steps": [{
        |		"step": [{
        |			"label": "test1"
        |		}]
        |	}]
        |}
      """.stripMargin
    sql"""INSERT INTO buckets (experiment_id, modular, http_verb, api_path, request_body, timeout, impression_id, is_graph_query, is_empty) VALUES (
          ${expId},
          "1~100",
          "POST",
          ${graphUrl + "/graphs/getEdges"},
          ${body},
          1000,
          "test_bucket",
          1,
          0
         )""".updateAndReturnGeneratedKey().apply().toInt
  }

  def destroyBucket(): Unit = {
    implicit val dbSession = AutoSession

    sql"""DELETE FROM buckets""".update().apply()
    sql"""DELETE FROM experiments""".update().apply()
  }

  override def beforeAll: Unit = {
    if (Management.findService("test").isEmpty) {
      Management.createService("test", graphConfig.HBASE_ZOOKEEPER_QUORUM, "test", 1, None, "gz")
    }
    val service = Management.findService("test").get

    // original label
    if (Management.findLabel("test1").isEmpty) {
      Management.createLabel("test1", "test", "c1", "string", "test", "c1", "string", true, "test", Nil, Nil, "weak", None, None, isAsync = false)
    }
    val l1Id = Management.findLabel("test1").get.id.get

    // transform label
    if (Management.findLabel("test2").isEmpty) {
      Management.createLabel("test2", "test", "c1", "string", "test", "c1", "string", true, "test", Nil, Nil, "weak", None, None, isAsync = false)
    }
    val l2Id = Management.findLabel("test2").get.id.get

    // transform label for changing src vertex id
    if (Management.findLabel("test3").isEmpty) {
      Management.createLabel("test3", "test", "c1", "string", "test", "c1", "string", true, "test", Nil, Nil, "weak", None, None, isAsync = false)
    }
    val l3Id = Management.findLabel("test3").get.id.get

    // edge for etl query
    // this will transform a vertex 'a' into '0'
    val edge = "100\tinsert\te\ta\t0\ttest1"
    Await.result(_rest.post(BulkWithWaitRequest(edge)), 10 seconds)

    Etl.create(l1Id.toInt, l2Id.toInt)
    destroyBucket()
    val bucketId = makeBucket(service)
    val bucketJson =
      s"""
         |{
         |  "type": "bucket",
         |  "value": "$bucketId"
         |}
       """.stripMargin
    Etl.create(l1Id.toInt, l3Id.toInt, Some(bucketJson))
  }

  override def afterAll: Unit = {
    Etl.findByOriginalLabelIds(Management.findLabel("test1").get.id.get).foreach { etl =>
      Etl.delete(etl.id)
    }

    destroyBucket()

    Management.deleteLabel("test3")
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

    val result = _edgeTransform.extractTargetVertex(json)
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

    val result = _edgeTransform.extractTargetVertex(json)
    result.isSuccess should equal(true)
    result.get should equal("1")
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

  it should "transform an edge" in {
    val e1 = Management.toEdge(1, "insert", "a", "0", "test1", "out", "{}")
    val future = _edgeTransform.transformEdge(e1)

    val result = Await.result(future, 10 seconds)
    result should not be empty

    val e2 = result.head
    e2.label.label should equal("test2")

    val rets = Await.result(_s2graph.mutateEdges(Seq(e2), withWait = true), 10 seconds)
    rets should not be empty

    rets.head should equal(true)
  }

  it should "transform an edge rest api" in {
    val orgLabel = Management.findLabel("test1").get
    val etls = Etl.findByOriginalLabelIds(orgLabel.id.get)
    etls should have length 2
//    logger.info(s"$etls")

    val e1 = Management.toEdge(1, "insert", "a", "1", "test1", "out", "{}")
    val future = _edgeTransform.transformEdge(e1)

    val result = Await.result(future, 10 seconds)
    result.map { r => r.label.label } should (have length 2 and contain only ("test2", "test3"))
    result.map { r => r.srcVertex.innerId.toIdString() } should contain only ("a", "0")

    val rets = Await.result(_edgeTransform.loadEdges(result, withWait = true), 10 seconds)
    rets should not be empty

    rets.head should equal(true)

    // query test2 label's edge from vertex 'a'
    val request1 = GetEdgesRequest(Json.obj(
      "srcVertices" -> Json.arr(
        Json.obj(
          "serviceName" -> "test",
          "columnName" -> "c1",
          "id" -> "a"
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
    val value1 = Await.result(
      for {
        ret <- _rest.post(request1).map(_.json)
      } yield ret,
      10 seconds
    )

//    logger.info(s"$value1")

    _edgeTransform.extractTargetVertex(value1).get should equal("1")

    // query test3 label's edge from vertex '0'
    val request2 = GetEdgesRequest(Json.obj(
      "srcVertices" -> Json.arr(
        Json.obj(
          "serviceName" -> "test",
          "columnName" -> "c1",
          "id" -> "0"
        )
      ),
      "steps" -> Json.arr(
        Json.obj(
          "step" -> Json.arr(
            Json.obj(
              "label" -> "test3"
            )
          )
        )
      )
    ))
    val value2 = Await.result(
      for {
        ret <- _rest.post(request2).map(_.json)
      } yield ret,
      10 seconds
    )

//    logger.info(s"$value2")

    _edgeTransform.extractTargetVertex(value2).get should equal("1")
  }

  it should "skip edge if label is not unregistered" in {
    val e1 = Management.toEdge(1, "insert", "2", "0", "test2", "out", "{}")
    val future = _edgeTransform.transformEdge(e1)

    val result = Await.result(future, 10 seconds)
    result shouldBe empty
  }
}
