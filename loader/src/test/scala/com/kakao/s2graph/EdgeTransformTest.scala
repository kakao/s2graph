package com.kakao.s2graph

import com.kakao.s2graph.core.mysqls.{Etl, Label, Model, Service}
import com.kakao.s2graph.core.rest.RestCaller
import com.kakao.s2graph.core.{Graph, GraphConfig, Management}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 22..
  */
class EdgeTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val graphEx = ExecutionContext.Implicits.global

  lazy val config = ConfigFactory.load()
  val graphConfig = new GraphConfig(config)
  Model(config)
  lazy val _s2graph = new Graph(config)
  lazy val _rest = new RestCaller(_s2graph)
  lazy val _edgeTransform = new EdgeTransform(_rest)

  override def beforeAll: Unit = {
    if (Service.findByName("test").isEmpty) {
      Service.insert("test", "localhost", "test", 1, None, "gz")
    }
    val service = Service.findByName("test").get

    if (Label.findByName("test1").isEmpty) {
      Label.insert("test1", service.id.get, "c1", "string", service.id.get, "c1", "string", true, "test", service.id.get, "weak", "test", None, "v3", false, "gz")
    }
    val l1Id = Label.findByName("test1").get.id.get
    if (Label.findByName("test2").isEmpty) {
      Label.insert("test2", service.id.get, "c1", "string", service.id.get, "c1", "string", true, "test", service.id.get, "weak", "test", None, "v3", false, "gz")
    }
    val l2Id = Label.findByName("test2").get.id.get
    Etl.create(l1Id.toInt, l2Id.toInt)
  }

  "EdgeTransform" should "change label" in {
    val e1 = Management.toEdge(1, "insert", "0", "1", "test1", "out", "{}")
    val future = _edgeTransform.changeEdge(e1)

    val result = Await.result(future, 10 seconds)
    result should not be empty
    result.get.label.label should equal("test2")
  }
}
