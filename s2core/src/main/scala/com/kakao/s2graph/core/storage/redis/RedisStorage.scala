package com.kakao.s2graph.core.storage.redis

import com.google.common.cache.Cache
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.hbase.{IndexEdgeDeserializable, VertexDeserializable, SnapshotEdgeDeserializable}
import com.kakao.s2graph.core.storage.{StorageDeserializable, StorageSerializable, Storage}
import com.typesafe.config.Config

import scala.concurrent.{Future, ExecutionContext}

/**
  * Redis storage handler class
  *
  * Created by june.kay on 2015. 12. 31..
  *
  */
class RedisStorage(val config: Config, vertexCache: Cache[Integer, Option[Vertex]])
                  (implicit ec: ExecutionContext) extends Storage {

  // initialize just once ( so we use `val` not `def` ) -> validate once
  val cacheOpt = None

  val snapshotEdgeDeserializer = new SnapshotEdgeDeserializable
  val indexEdgeDeserializer = new IndexEdgeDeserializable
  val vertexDeserializer = new VertexDeserializable

  val queryBuilder = new RedisQueryBuilder(this)(ec)

  // Serializer/Deserializer
  def indexEdgeSerializer(indexedEdge: IndexEdge): StorageSerializable[IndexEdge] =
    new RedisIndexEdgeSerializable(indexedEdge)
  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): StorageSerializable[SnapshotEdge] =
    new RedisSnapshotEdgeSerializable(snapshotEdge)
  def vertexSerializer(vertex: Vertex): StorageSerializable[Vertex] =
    new RedisVertexSerializable(vertex)

  override def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = {
    val futures = for {
      (srcVertex, tgtVertex, queryParam) <- params
    } yield queryBuilder.getEdge(srcVertex, tgtVertex, queryParam, false)

    Future.sequence(futures)
  }

  override def flush(): Unit = ???

  override def vertexCacheOpt: Option[Cache[Integer, Option[Vertex]]] = ???


  // Interface
  override def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = ???

  override def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] = ???

  override def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = ???

  override def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = ???

  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = ???

  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = ???
}
