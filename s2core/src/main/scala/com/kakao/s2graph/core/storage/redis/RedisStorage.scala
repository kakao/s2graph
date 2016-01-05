package com.kakao.s2graph.core.storage.redis

import com.google.common.cache.Cache
import com.kakao.s2graph.core.mysqls.Label
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.hbase.{IndexEdgeDeserializable, VertexDeserializable, SnapshotEdgeDeserializable}
import com.kakao.s2graph.core.storage._
import com.kakao.s2graph.core.utils.AsyncRedisClient
import com.typesafe.config.Config
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}

/**
  * Redis storage handler class
  *
  * Created by june.kay on 2015. 12. 31..
  *
  */
class RedisStorage(val config: Config, vertexCache: Cache[Integer, Option[Vertex]])
                  (implicit ec: ExecutionContext) extends Storage {


  implicit val akkaSystem = akka.actor.ActorSystem()
  lazy val client = new AsyncRedisClient(config)

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

  def get(get: RedisGet): Future[Set[SKeyValue]] = {
    Future[Set[SKeyValue]] {
      // send rpc call to Redis instance
      client.doBlockWithKey[Set[SKeyValue]]("" /* sharding key */) { jedis =>
        jedis.zrangeByLex(get.key, get.min, get.max, get.offset, get.count).toSet[Array[Byte]].map(v =>
          SKeyValue(Array.empty[Byte], get.key, Array.empty[Byte], Array.empty[Byte], v, 0L)
        )
      } match {
        case Success(v) => v
        case Failure(e) => Set[SKeyValue]()
      }
    }
  }


  // Interface
  override def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = ???

  override def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] = ???

  override def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = ???

  override def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = ???

  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = ???

  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = ???
}
