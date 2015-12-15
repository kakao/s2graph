package com.kakao.s2graph

import com.kakao.s2graph.core.types.v2.InnerVal
import com.kakao.s2graph.core.types.{InnerValLikeWithTs, VertexId}
import com.kakao.s2graph.core.{Vertex, Edge}
import com.kakao.s2graph.core.mysqls.{Bucket, Etl}
import com.typesafe.config.Config
import play.api.libs.json.{JsNull, JsObject, JsValue}

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
class EdgeTransform(config: Config) {
  def changeEdge(edge: Edge): Option[Edge] = {
    Some(edge)
  }

  def changeEdges(edges: Seq[Edge]): Seq[Edge] = {
    for {
      edge <- edges if edge.label.id.isDefined
      labelId = edge.label.id.get
      etl <- Etl.findByOriginalLabelIds(labelId)
    } yield {
      val newSrc = etl.srcEtlQueryId.map { queryId =>
        Vertex(VertexId(-1, InnerVal(-1)))
      }
//      etl.tgtEtlQueryId
//      etl.propEtlQueryId
      // change src or target
      // merge property
      edge
    }
  }

  def runQuery(queryId: Int, param: String): Option[JsValue] = {
    Bucket.findById(queryId).map { bucket =>
      // build query body
//      bucket.requestBody.replace("#uuid", param)
      JsNull
    }
  }
}
