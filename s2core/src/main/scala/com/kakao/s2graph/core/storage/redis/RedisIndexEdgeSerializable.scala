package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.types.v2.InnerVal
import com.kakao.s2graph.core.{GraphUtil, IndexEdge}
import com.kakao.s2graph.core.storage.{SKeyValue, StorageSerializable}
import com.kakao.s2graph.core.types.VertexId
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author Junki Kim (wishoping@gmail.com), Hyunsung Jo (hyunsung.jo@gmail.com) on 1/7/16.
 */
class RedisIndexEdgeSerializable(indexEdge: IndexEdge) extends StorageSerializable[IndexEdge]{
  import StorageSerializable._

  val label = indexEdge.label

  val idxPropsMap = indexEdge.orders.toMap
  val idxPropsBytes = propsToBytes(indexEdge.orders)

  def toKeyValues: Seq[SKeyValue] = {
    val _srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
    val srcIdBytes = _srcIdBytes.takeRight(_srcIdBytes.length - GraphUtil.bytesForMurMurHash)
    val labelWithDirBytes = indexEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
    val _tgtIdBytes = VertexId.toTargetVertexId(indexEdge.tgtVertex.id).bytes
    val tgtIdBytes = _tgtIdBytes.takeRight(_tgtIdBytes.length - GraphUtil.bytesForMurMurHash)
    val qualifier =
      if (indexEdge.op == GraphUtil.operations("incrementCount")) {
        // TODO
        Bytes.add(idxPropsBytes, tgtIdBytes, Array.fill(1)(indexEdge.op))
      } else {
        idxPropsMap.get(LabelMeta.toSeq) match {
          case None => Bytes.add(idxPropsBytes, tgtIdBytes)
          case Some(vId) => idxPropsBytes
        }
      }
    val qualifierLen = Array.fill[Byte](1)(qualifier.length.toByte)
    val timestamp = InnerVal(indexEdge.ts).bytes
    val propsKv = propsToKeyValues(indexEdge.metas.toSeq)
    val value = qualifierLen ++ qualifier ++ timestamp ++ tgtIdBytes ++ propsKv
    val emptyArray = Array.empty[Byte]
    val kv = SKeyValue(emptyArray, row, emptyArray, emptyArray, value, indexEdge.version)

    Seq(kv)
  }
}
