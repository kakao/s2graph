package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.Vertex
import com.kakao.s2graph.core.storage.{SKeyValue, StorageSerializable}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by june.kay on 2015. 12. 31..
  */
class RedisVertexSerializable(vertex: Vertex) extends StorageSerializable[Vertex] {
  override def toKeyValues: Seq[SKeyValue] = {
    val _rowBytes = vertex.id.bytes
    val row = _rowBytes.takeRight(_rowBytes.length-2) // eliminate hash bytes
    val base = for ((k, v) <- vertex.props ++ vertex.defaultProps) yield Bytes.toBytes(k) -> v.bytes
    val belongsTo = vertex.belongLabelIds.map { labelId => Bytes.toBytes(Vertex.toPropKey(labelId)) -> Array.empty[Byte] }
    val emptyArray = Array.empty[Byte]
    (base ++ belongsTo).map { case (qualifier, value) =>
      val qualifierWithTs = qualifier ++ Bytes.toBytes(vertex.ts)
      SKeyValue(emptyArray, row, emptyArray, qualifierWithTs, value, 0)
    } toSeq
  }
}
