package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import com.kakao.s2graph.core.{IndexEdge, Vertex, TestCommonWithModels}
import com.kakao.s2graph.core.types._
import org.scalatest.{FunSuite, Matchers}


class IndexEdgeTest extends FunSuite with Matchers with TestCommonWithModels {
  initTests()
  /** note that props have to be properly set up for equals */
  test("test serializer/deserializer for index edge.") {
    val ts = System.currentTimeMillis()
    val from = InnerVal.withLong(1, label.schemaVersion)
    val to = InnerVal.withLong(101, label.schemaVersion)
    val vertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId, ts)
    val tgtVertex = Vertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(label.id.get, 0)

    val tsInnerVal = InnerVal.withLong(ts, label.schemaVersion)
    val props = Map(LabelMeta.timeStampSeq -> tsInnerVal,
      1.toByte -> InnerVal.withDouble(2.1, label.schemaVersion))
    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val _indexEdge = graph.storage.indexEdgeDeserializer.fromKeyValues(queryParam,
      graph.storage.indexEdgeSerializer(indexEdge).toKeyValues, label.schemaVersion, None)
    println(indexEdge)
    println(_indexEdge)
    indexEdge should be(_indexEdge)

  }

  test("test serializer/deserializer for degree edge.") {
    val ts = System.currentTimeMillis()
    val from = InnerVal.withLong(1, label.schemaVersion)
    val to = InnerVal.withStr("0", label.schemaVersion)
    val vertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId, ts)
    val tgtVertex = Vertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(label.id.get, 0)

    val tsInnerVal = InnerVal.withLong(ts, label.schemaVersion)
    val props = Map(
      LabelMeta.degreeSeq -> InnerVal.withLong(10, label.schemaVersion),
      LabelMeta.timeStampSeq -> tsInnerVal)

    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val _indexEdge = graph.storage.indexEdgeDeserializer.fromKeyValues(queryParam,
      graph.storage.indexEdgeSerializer(indexEdge).toKeyValues, label.schemaVersion, None)
    println(indexEdge)
    println(_indexEdge)
    indexEdge should be(_indexEdge)

  }

  test("test serializer/deserializer for incrementCount index edge.") {
    val ts = System.currentTimeMillis()
    val from = InnerVal.withLong(1, label.schemaVersion)
    val to = InnerVal.withLong(101, label.schemaVersion)
    val vertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, from)
    val tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, to)
    val vertex = Vertex(vertexId, ts)
    val tgtVertex = Vertex(tgtVertexId, ts)
    val labelWithDir = LabelWithDirection(label.id.get, 0)

    val tsInnerVal = InnerVal.withLong(ts, label.schemaVersion)
    val props = Map(LabelMeta.timeStampSeq -> tsInnerVal,
      1.toByte -> InnerVal.withDouble(2.1, label.schemaVersion),
      LabelMeta.countSeq -> InnerVal.withLong(10, label.schemaVersion))
    
    val indexEdge = IndexEdge(vertex, tgtVertex, labelWithDir, 0, ts, LabelIndex.DefaultSeq, props)
    val _indexEdge = graph.storage.indexEdgeDeserializer.fromKeyValues(queryParam,
      graph.storage.indexEdgeSerializer(indexEdge).toKeyValues, label.schemaVersion, None)
    println(indexEdge)
    println(_indexEdge)
    indexEdge should be(_indexEdge)

  }
}
