package com.kakao.s2graph.core.storage.rocks

import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{StorageSerializable, SKeyValue, QueryBuilder}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.Map
import scala.collection.mutable.{ListBuffer}
import scala.concurrent.{Future, ExecutionContext}

class RocksDBQueryBuilder(storage: RocksDBStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[Future[QueryRequestWithResult]](storage) {

  val emptyBytes = Array.empty[Byte]
  val table = Array.empty[Byte]
  val cf = Array.empty[Byte]
  val qualifier = Array.empty[Byte]

  /** actually there is no need for wrap this into Future. just to want to use future cache here */
  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = Future {
    val queryParam = queryRequest.queryParam
    val edge = toRequestEdge(queryRequest)
    val indexEdgeOpt = edge.edgesWithIndex.filter(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq).headOption
    val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))
    val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
    val labelWithDirBytes = indexEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

    val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, Bytes.add(labelIndexSeqWithIsInvertedBytes, Array.fill(1)(edge.op)))
    val (startKey, stopKey) = (baseKey, Bytes.add(baseKey, Array.fill(1)(-1)))
    val iter = storage.db.newIterator()
    val kvs = new ListBuffer[SKeyValue]()
    var idx = 0
    iter.seek(startKey)
    while (iter.isValid && Bytes.compareTo(iter.key(), stopKey) <= 0 && idx < queryParam.limit) {
      kvs += SKeyValue(table, iter.key, cf, qualifier, iter.value, System.currentTimeMillis())
      idx += 1
    }
    val edgeWithScores = storage.toEdges(kvs.toSeq, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
    val resultEdgesWithScores =
      if (queryRequest.queryParam.sample >= 0 ) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
      else edgeWithScores

    QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.last.row))
  }

  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
                       prevStepEdges: Map[VertexId,
                       Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = {
    val futures = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
      parentEdges <- prevStepEdges.get(queryRequest.vertex.id)
    } yield fetch(queryRequest, prevStepScore, isInnerCall = false, parentEdges)

    Future.sequence(futures)
  }

  override def fetchSnapshotEdge(edge: Edge): Future[(QueryParam, Option[Edge], Option[SKeyValue])] = Future {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)
    val kv = storage.snapshotEdgeSerializer(edge.toSnapshotEdge).toKeyValues.head

    val iter = storage.db.newIterator()
    iter.seek(kv.row)
    val (edgeOpt, kvOpt) =
      if (iter.isValid) {
        val kv = SKeyValue(table, iter.key, cf, qualifier, iter.value, System.currentTimeMillis())
        val _edgeOpt = storage.toSnapshotEdge(kv, queryParam, None, isInnerCall = true, parentEdges = Nil).headOption
        val _kvOpt = Option(kv)
        (_edgeOpt, _kvOpt)
      } else (None, None)

    (queryParam, edgeOpt, kvOpt)
  }
}
