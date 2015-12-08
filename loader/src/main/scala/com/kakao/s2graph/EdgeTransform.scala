package com.kakao.s2graph

import com.kakao.s2graph.core.Edge
import com.typesafe.config.Config

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
class EdgeTransform(config: Config) {
  def changeEdge(edge: Edge): Option[Edge] = {
    Some(edge)
  }

  def changeEdges(edges: Seq[Edge]): Seq[Edge] = {
    edges
  }
}
