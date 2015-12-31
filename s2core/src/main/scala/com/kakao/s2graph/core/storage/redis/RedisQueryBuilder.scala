package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.hbase.SnapshotEdgeSerializable
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{SKeyValue, QueryBuilder}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.Map
import scala.concurrent.{Future, ExecutionContext}

/**
  * Created by june.kay on 2015. 12. 31..
  */
class RedisQueryBuilder(storage: RedisStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[RedisGet, Future[QueryRequestWithResult]](storage) {

  def buildRequest(queryRequest: QueryRequest): RedisGet = {
    val srcVertex = queryRequest.vertex

    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /** we use toSnapshotEdge so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertexId, tgtColumn.columnType, label.schemaVersion)
        (src, tgt)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        (src, src)
    }

    val (srcVId, tgtVId) = (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val currentTs = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs)).toMap
    val edge = Edge(srcV, tgtV, labelWithDir, propsWithTs = propsWithTs)

    val kv = if (tgtVertexIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues.head
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      storage.indexEdgeSerializer(indexedEdge).toKeyValues.head
    }

    val row = kv.row
    val key = row.take(4) ++ label.hTableName.getBytes ++ row.takeRight(row.length-4)

    // 1. RedisGet instance initialize
    val get = new RedisGet(key)

    // 2. set filter
    // 3. min/max value's key build
    val (minTs, maxTs) = queryParam.duration.getOrElse(-1L -> -1L)
    val (min, max) = (queryParam.columnRangeFilterMinBytes, queryParam.columnRangeFilterMaxBytes)
    get.setCount(queryParam.limit)
      .setOffset(queryParam.offset)
      .setTimeout(queryParam.rpcTimeoutInMillis)
      .setFilter(min, true, max, true, minTs, maxTs)
  }

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Future[QueryRequestWithResult] = ???

  def fetch(queryRequest: QueryRequest, prevStepScore: Double, isInnerCall: Boolean, parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = ???

  def toCacheKeyBytes(request: RedisGet): Array[Byte] = ???

  def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = ???
}
