package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.mysqls.{LabelMeta, LabelIndex}
import com.kakao.s2graph.core.types.{SourceVertexId, LabelWithDirection, SourceAndTargetVertexIdPair, HBaseType}
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{StorageDeserializable, CanSKeyValue, SKeyValue}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by june.kay on 2016. 1. 5..
  */
class RedisSnapshotEdgeDeserializable extends RDeserializable[SnapshotEdge]  {
  import StorageDeserializable._

  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, kvs: Seq[T], version: String, cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge = {
    queryParam.label.schemaVersion match {
      case HBaseType.VERSION3 => fromKeyValuesInnerV3(queryParam, kvs, version, cacheElementOpt)
      case _ => throw new GraphExceptions.NotSupportedSchemaVersion(">> Redis storage engine can support only v3.")
    }
  }

  def statusCodeWithOp(byte: Byte): (Byte, Byte) = {
    val statusCode = byte >> 4
    val op = byte & ((1 << 4) - 1)
    (statusCode.toByte, op.toByte)
  }

  private def fromKeyValuesInnerV3[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[SnapshotEdge]): SnapshotEdge = {
    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    assert(kvs.size == 1)

    val kv = kvs.head
    val schemaVer = queryParam.label.schemaVersion
    val cellVersion = kv.timestamp

    /**
      * put leading hash bytes for compatibility with legacy codes
      * @param v Byte array to pad
      * @return 2 bytes padded bytes array
      */
    def paddingHashBytes(v: Array[Byte]): Array[Byte] = Bytes.add( Array.fill[Byte](2)(0), v)

    /** rowKey */
    def parseRowV3(kv: SKeyValue, version: String) = {
      var pos = 0
      val (srcIdAndTgtId, srcIdAndTgtIdLen) = SourceAndTargetVertexIdPair.fromBytes(paddingHashBytes(kv.row), pos, kv.row.length, version)
      pos += srcIdAndTgtIdLen
      val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
      pos += 4
      val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)

      val rowLen = srcIdAndTgtIdLen + 4 + 1
      (srcIdAndTgtId.srcInnerId, srcIdAndTgtId.tgtInnerId, labelWithDir, labelIdxSeq, isInverted, rowLen)

    }
    val (srcInnerId, tgtInnerId, labelWithDir, _, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.innerId, e.tgtVertex.innerId, e.labelWithDir, LabelIndex.DefaultSeq, true, 0)
    }.getOrElse(parseRowV3(kv, schemaVer))

    val srcVertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, srcInnerId)
    val tgtVertexId = SourceVertexId(HBaseType.DEFAULT_COL_ID, tgtInnerId)

    val (props, op, ts, statusCode, _pendingEdgeOpt) = {
      var pos = 0
      val (statusCode, op) = statusCodeWithOp(kv.value(pos))
      pos += 1
      val (props, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
      val kvsMap = props.toMap
      val ts = kvsMap(LabelMeta.timeStampSeq).innerVal.toString.toLong

      pos = endAt
      val _pendingEdgeOpt =
        if (pos == kv.value.length) None
        else {
          val (pendingEdgeStatusCode, pendingEdgeOp) = statusCodeWithOp(kv.value(pos))
          pos += 1
          val (pendingEdgeProps, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer)
          pos = endAt
          val lockTs = Option(Bytes.toLong(kv.value, pos, 8))

          val pendingEdge =
            Edge(Vertex(srcVertexId, cellVersion),
              Vertex(tgtVertexId, cellVersion),
              labelWithDir, pendingEdgeOp,
              cellVersion, pendingEdgeProps.toMap,
              statusCode = pendingEdgeStatusCode, lockTs = lockTs)
          Option(pendingEdge)
        }

      (kvsMap, op, ts, statusCode, _pendingEdgeOpt)
    }

    SnapshotEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts),
      labelWithDir, op, cellVersion, props, statusCode = statusCode,
      pendingEdgeOpt = _pendingEdgeOpt, lockTs = None)
  }
}
