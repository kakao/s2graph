package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.types.InnerVal

import scala.collection.Seq
import scala.concurrent.ExecutionContext
import org.apache.hadoop.hbase.util.Bytes

abstract class MutationBuilder[T](storage: Storage)(implicit ex: ExecutionContext) {
  /** operation that needs to be supported by backend persistent storage system */
  def put(kvs: Seq[SKeyValue]): Seq[T]

  def increment(kvs: Seq[SKeyValue]): Seq[T]

  def delete(kvs: Seq[SKeyValue]): Seq[T]


  /** build mutation for backend persistent storage system */

  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[T] = {
    val deleteMutations = edgeMutate.edgesToDelete.flatMap(edge => buildDeletesAsync(edge))
    val insertMutations = edgeMutate.edgesToInsert.flatMap(edge => buildPutsAsync(edge))

    deleteMutations ++ insertMutations
  }

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[T] =
    edgeMutate.newSnapshotEdge.map(e => buildPutAsync(e)).getOrElse(Nil)

  def increments(edgeMutate: EdgeMutate): Seq[T] =
    (edgeMutate.edgesToDelete.isEmpty, edgeMutate.edgesToInsert.isEmpty) match {
      case (true, true) =>

        /** when there is no need to update. shouldUpdate == false */
        List.empty
      case (true, false) =>

        /** no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeMutate.edgesToInsert.flatMap { e => buildIncrementsAsync(e) }
      case (false, true) =>

        /** no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeMutate.edgesToDelete.flatMap { e => buildIncrementsAsync(e, -1L) }
      case (false, false) =>

        /** update on existing edges so no change on degree */
        List.empty
    }

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[T] = {
    val newProps = indexedEdge.props ++ Map(LabelMeta.degreeSeq -> InnerVal.withLong(amount, indexedEdge.schemaVer))
    val _indexedEdge = indexedEdge.copy(props = newProps)
    increment(storage.indexEdgeSerializer(_indexedEdge).toKeyValues)
  }

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[T] = {
    val newProps = indexedEdge.props ++ Map(LabelMeta.countSeq -> InnerVal.withLong(amount, indexedEdge.schemaVer))
    val _indexedEdge = indexedEdge.copy(props = newProps)
    increment(storage.indexEdgeSerializer(_indexedEdge).toKeyValues)
  }

  def buildDeletesAsync(indexedEdge: IndexEdge): Seq[T] = delete(storage.indexEdgeSerializer(indexedEdge).toKeyValues)

  def buildPutsAsync(indexedEdge: IndexEdge): Seq[T] = put(storage.indexEdgeSerializer(indexedEdge).toKeyValues)

  /** SnapshotEdge */
  def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[T] = put(storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues)

  def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[T] = delete(storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues)

  /** Vertex */
  def buildPutsAsync(vertex: Vertex): Seq[T] = put(storage.vertexSerializer(vertex).toKeyValues)

  def buildDeleteAsync(vertex: Vertex): Seq[T] = delete(Seq(storage.vertexSerializer(vertex).toKeyValues.head.copy(qualifier = null)))

  def buildDeleteBelongsToId(vertex: Vertex): Seq[T] = {
    val kvs = storage.vertexSerializer(vertex).toKeyValues
    val kv = kvs.head


    val newKVs = vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(Vertex.toPropKey(id)))
    }
    delete(newKVs)
  }

  def buildVertexPutsAsync(edge: Edge): Seq[T] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      buildPutsAsync(edge.srcForVertex) ++ buildPutsAsync(edge.tgtForVertex)

  def buildPutsAll(vertex: Vertex): Seq[T] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => buildDeleteAsync(vertex)
      case _ => buildPutsAsync(vertex)
    }
  }

}
