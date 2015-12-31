package com.kakao.s2graph

import com.kakao.s2graph.client.{ExperimentRequest, GraphRestClient}
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.{Edge, GraphUtil, Management}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
class EdgeTransform(rest: GraphRestClient)(implicit ec: ExecutionContext) {
  val log = LoggerFactory.getLogger(getClass)

  def changeEdge(edge: Edge): Future[Option[Edge]] = {
    changeEdges(Seq(edge)).map { seq =>
      seq.headOption
    }
  }

  def changeEdges(edges: Seq[Edge]): Future[Seq[Edge]] = Future.sequence {
    for {
      edge <- edges if edge.label.id.isDefined
      labelId = edge.label.id.get
      etl <- Etl.findByOriginalLabelIds(labelId)
      transformLabel <- Label.findByIdOpt(etl.transformLabelId)
      src = edge.srcVertex.innerId.toIdString()
      tgt = edge.tgtVertex.innerId.toIdString()
      propsWithName = edge.propsWithName
    } yield {
      val payload = Json.obj(
        "[[from]]" -> src,
        "[[to]]" -> tgt
      )
      // change src or target
      val srcFuture = etl.srcEtlQueryId match {
        case Some(queryId) =>
          runQuery(payload, queryId, src).map { js =>
            extractTargetVertex(js).getOrElse(src)
          }
        case None =>
          Future.successful(src)
      }
      val tgtFuture = etl.tgtEtlQueryId match {
        case Some(queryId) =>
          runQuery(payload, queryId, tgt).map { js =>
            extractTargetVertex(js).getOrElse(tgt)
          }
        case None =>
          Future.successful(tgt)
      }
      val propFuture = etl.propEtlQueryId match {
        case Some(queryId) =>
          runQuery(payload, queryId, "").map { js =>
            extractProps(js).map { newProps =>
              propsWithName ++ newProps.as[JsObject].fields.toMap
            }.getOrElse(propsWithName)
          }
        case None =>
          Future.successful(propsWithName)
      }

      for {
        newSrc <- srcFuture
        newTgt <- tgtFuture
        newProps <- propFuture
      } yield Management.toEdge(
        edge.ts,
        GraphUtil.fromOp(edge.op),
        src,
        tgt,
        transformLabel.label,
        "out",
        Json.toJson(newProps).toString()
      )
    }
  }

  private def runQuery(payload: JsValue, queryId: Int, uuid: String): Future[JsValue] = {
    for {
      bucket <- Bucket.findById(queryId)
      experiment <- Experiment.findById(bucket.experimentId)
      service <- Try { Service.findById(experiment.serviceId) }.toOption
    } yield ExperimentRequest(service.accessToken, experiment.name, uuid, payload)
  } match {
    case Some(req) => rest.post(req).map(_.json)
    case None => Future.failed(new RuntimeException("cannot find experiment"))
  }

  private def extractTargetVertex(js: JsValue): JsResult[String] = {
    ((js \ "results")(0) \ "to").validate[String]
  }

  private def extractProps(js: JsValue): JsResult[JsObject] = {
    ((js \ "results")(0) \ "props").validate[JsObject]
  }
}
