package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{SKeyValue, MutationBuilder}
import com.kakao.s2graph.core.utils.logger
import org.apache.hadoop.hbase.util.Bytes

import scala.concurrent.ExecutionContext

class RedisMutationBuilder(storage: RedisStorage)(implicit ec: ExecutionContext)
  extends MutationBuilder[RedisRPC](storage) {

  def put(kvs: Seq[SKeyValue]): Seq[RedisRPC] =
    kvs.map { kv => new RedisPutRequest(kv.row, kv.qualifier, kv.value, kv.timestamp) }

  def increment(kvs: Seq[SKeyValue]): Seq[RedisRPC] =
    kvs.map { kv => new RedisAtomicIncrementRequest(kv.row, kv.value, Bytes.toLong(kv.value)) }

  def delete(kvs: Seq[SKeyValue]): Seq[RedisRPC] =
    kvs.map { kv =>
      if (kv.qualifier == null) new RedisDeleteRequest(kv.row, kv.value, kv.timestamp)
      else new RedisDeleteRequest(kv.row, kv.value, kv.timestamp)
    }

  /** Vertex */
  def buildPutsAsync(vertex: Vertex): Seq[RedisRPC] = {
    val kvs = storage.vertexSerializer(vertex).toKeyValues
    put(kvs)
  }

  def buildDeleteBelongsToId(vertex: Vertex): Seq[RedisRPC] = {
    val kvs = storage.vertexSerializer(vertex).toKeyValues
    val kv = kvs.head

    import org.apache.hadoop.hbase.util.Bytes
    val newKVs = vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(Vertex.toPropKey(id)))
    }
    delete(newKVs)
  }

  def increments(edgeMutate: EdgeMutate): Seq[RedisRPC] = {
    (edgeMutate.edgesToDelete.isEmpty, edgeMutate.edgesToInsert.isEmpty) match {
      case (true, true) =>

        /** when there is no need to update. shouldUpdate == false */
        List.empty[RedisAtomicIncrementRequest]
      case (true, false) =>

        /** no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeMutate.edgesToInsert.flatMap { e => buildIncrementsAsync(e) }
      case (false, true) =>

        /** no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeMutate.edgesToDelete.flatMap { e => buildIncrementsAsync(e, -1L) }
      case (false, false) =>

        /** update on existing edges so no change on degree */
        List.empty[RedisAtomicIncrementRequest]
    }
  }


  def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[RedisRPC] = ???

  def buildDeleteAsync(vertex: Vertex): Seq[RedisRPC] = ???

  def buildVertexPutsAsync(edge: Edge): Seq[RedisRPC] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      buildPutsAsync(edge.srcForVertex) ++ buildPutsAsync(edge.tgtForVertex)

  def buildDeletesAsync(indexedEdge: IndexEdge): Seq[RedisRPC] = ???

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long): Seq[RedisRPC] =
    storage.indexEdgeSerializer(indexedEdge).toKeyValues.headOption match {
      case None => Nil
      case Some(kv) =>
        val zeroLenBytes = Array.fill[Byte](1)(0)
        val copiedKV = kv.copy(qualifier = Array.empty[Byte], value = Bytes.add(zeroLenBytes, Bytes.toBytes(amount)))
        increment(Seq(copiedKV))
    }


  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[RedisRPC] = ???

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long): Seq[RedisRPC] = ???

  def buildPutsAsync(indexedEdge: IndexEdge): Seq[RedisRPC] =
    put(storage.indexEdgeSerializer(indexedEdge).toKeyValues)

  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[RedisRPC] = {
    val deleteMutations = edgeMutate.edgesToDelete.flatMap(edge => buildDeletesAsync(edge))
    val insertMutations = edgeMutate.edgesToInsert.flatMap(edge => buildPutsAsync(edge))

    deleteMutations ++ insertMutations
  }

  /** SnapshotEdge */
  def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[RedisRPC] = {
    logger.error(s">> buildPutAsync")
    put(storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues)
  }

}