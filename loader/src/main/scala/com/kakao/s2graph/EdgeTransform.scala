package com.kakao.s2graph

import com.kakao.s2graph.core.mysqls.{Etl, Label}
import com.kakao.s2graph.core.rest.RestCaller
import com.kakao.s2graph.core.{Edge, GraphUtil, Management}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
class EdgeTransform(rest: RestCaller)(implicit ec: ExecutionContext) {
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
//      val newSrcOpt = for {
//        queryId <- etl.srcEtlQueryId
//        js <- runQuery(payload, queryId, src)
//        tgtId <- extractTargetVertex(js)
//      } yield tgtId
//      val newTgtOpt = for {
//        queryId <- etl.tgtEtlQueryId
//        js <- runQuery(payload, queryId, tgt)
//        tgtId <- extractTargetVertex(js)
//      } yield tgtId
//
//      // merge property
//      val newPropsOpt = for {
//        queryId <- etl.propEtlQueryId
//        js <- runQuery(payload, queryId, "")
//        props <- extractProps(js)
//        newProps = props.as[JsObject].fields.toMap
//      } yield edge.propsWithName ++ newProps

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
//
//      Management.toEdge(
//        edge.ts,
//        GraphUtil.fromOp(edge.op),
//        newSrcOpt.getOrElse(edge.srcVertex.innerId.toIdString()),
//        newTgtOpt.getOrElse(edge.tgtVertex.innerId.toIdString()),
//        transformLabel.label,
//        "out",
//        newPropsOpt.getOrElse(edge.propsWithName).toString()
//      )
    }
  }

  private def runQuery(payload: JsValue, queryId: Int, uuid: String): Future[JsValue] = {
    rest.bucket(payload, queryId, uuid).map(_._1)
  }

  private def extractTargetVertex(js: JsValue): Option[String] = {
    (js \ "results").as[Vector[JsValue]].headOption.flatMap { result =>
      result \ "to" match {
        case JsString(s) => Some(s)
        case JsNumber(n) => Some(n.toString())
        case _ =>
          log.error("incorrect vertex type")
          None
      }
    }
  }

  private def extractProps(js: JsValue): Option[JsValue] = {
    (js \ "results").as[Vector[JsValue]].headOption.map { result =>
      result \ "props"
    }
  }
}
