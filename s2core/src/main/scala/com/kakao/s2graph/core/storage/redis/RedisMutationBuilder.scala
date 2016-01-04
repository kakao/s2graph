package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{SKeyValue, MutationBuilder}

import scala.concurrent.ExecutionContext

class RedisMutationBuilder(storage: RedisStorage)(implicit ec: ExecutionContext)
  extends MutationBuilder[RedisRPC](storage) {

  def put(kvs: Seq[SKeyValue]): Seq[RedisRPC] = ???

  def increment(kvs: Seq[SKeyValue]): Seq[RedisRPC] = ???

  def buildPutsAsync(indexedEdge: IndexEdge): Seq[RedisRPC] = ???

  /** Vertex */
  def buildPutsAsync(vertex: Vertex): Seq[RedisRPC] = ???

  def buildDeleteBelongsToId(vertex: Vertex): Seq[RedisRPC] = ???

  def increments(edgeMutate: EdgeMutate): Seq[RedisRPC] = ???

  def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[RedisRPC] = ???

  def buildDeleteAsync(vertex: Vertex): Seq[RedisRPC] = ???

  def buildVertexPutsAsync(edge: Edge): Seq[RedisRPC] = ???

  def delete(kvs: Seq[SKeyValue]): Seq[RedisRPC] = ???

  def buildDeletesAsync(indexedEdge: IndexEdge): Seq[RedisRPC] = ???

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long): Seq[RedisRPC] = ???

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[RedisRPC] = ???

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long): Seq[RedisRPC] = ???

  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[RedisRPC] = ???

  /** SnapshotEdge */
  def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[RedisRPC] = ???
}