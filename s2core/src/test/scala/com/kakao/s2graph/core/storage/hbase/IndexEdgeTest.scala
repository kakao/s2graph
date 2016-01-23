package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import com.kakao.s2graph.core.{IndexEdge, Vertex, TestCommonWithModels}
import com.kakao.s2graph.core.types._
import org.scalatest.{FunSuite, Matchers}


class IndexEdgeTest extends FunSuite with Matchers with TestCommonWithModels {
  test("") {
    val from = InnerVal.withLong(1, label.schemaVersion)
    val to = InnerVal.withLong(101, label.schemaVersion)
    val vertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId)
    val tgtVertex = Vertex(tgtVertexId)
    val labelWithDir = LabelWithDirection(label.id.get, 0)
    val ts = System.currentTimeMillis()
    val tsInnerVal = InnerVal.withLong(ts, label.schemaVersion)
    val props = Map(LabelMeta.timeStampSeq -> tsInnerVal)
    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val kvs = graph.storage.indexEdgeSerializer(indexEdge).toKeyValues
    val _indexEdge = graph.storage.indexEdgeDeserializer.fromKeyValues(queryParam, kvs, label.schemaVersion, None)
    println(indexEdge)
    println(_indexEdge)
    indexEdge == _indexEdge
  }
}
