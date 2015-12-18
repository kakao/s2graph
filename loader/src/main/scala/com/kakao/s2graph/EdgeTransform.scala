package com.kakao.s2graph

import com.kakao.s2graph.core.mysqls.{Bucket, Etl, Label}
import com.kakao.s2graph.core.{Edge, GraphUtil, Management}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import play.api.libs.json._

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
class EdgeTransform(config: Config) {
  val log = LoggerFactory.getLogger(getClass)

  def changeEdge(edge: Edge): Option[Edge] = {
    changeEdges(Seq(edge)).headOption
  }

  def changeEdges(edges: Seq[Edge]): Seq[Edge] = {
    for {
      edge <- edges if edge.label.id.isDefined
      labelId = edge.label.id.get
      etl <- Etl.findByOriginalLabelIds(labelId)
      transformLabel <- Label.findByIdOpt(etl.transformLabelId)
    } yield {
      val params = Json.obj(
        "[[from]]" -> edge.srcVertex.innerId.toIdString(),
        "[[to]]" -> edge.tgtVertex.innerId.toIdString()
      )
      // change src or target
      val newSrcOpt = for {
        queryId <- etl.srcEtlQueryId
        js <- runQuery(queryId, params)
        tgtId <- extractTargetVertex(js)
      } yield tgtId
      val newTgtOpt = for {
        queryId <- etl.tgtEtlQueryId
        js <- runQuery(queryId, params)
        tgtId <- extractTargetVertex(js)
      } yield tgtId

      // merge property
      val newPropsOpt = for {
        queryId <- etl.propEtlQueryId
        js <- runQuery(queryId, params)
        props <- extractProps(js)
        newProps = props.as[JsObject].fields.toMap
      } yield edge.propsWithName ++ newProps

      Management.toEdge(
        edge.ts,
        GraphUtil.fromOp(edge.op),
        newSrcOpt.getOrElse(edge.srcVertex.innerId.toIdString()),
        newTgtOpt.getOrElse(edge.tgtVertex.innerId.toIdString()),
        transformLabel.label,
        "out",
        newPropsOpt.getOrElse(edge.propsWithName).toString()
      )
    }
  }

  def runQuery(queryId: Int, param: JsValue): Option[JsValue] = {
    Bucket.findById(queryId).map { bucket =>
      // build query body
//      bucket.requestBody.replace("#uuid", param)
      JsNull
    }
  }

  def extractTargetVertex(js: JsValue): Option[String] = {
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

  def extractProps(js: JsValue): Option[JsValue] = {
    (js \ "results").as[Vector[JsValue]].headOption.map { result =>
      result \ "props"
    }
  }
}
