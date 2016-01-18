package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.{SKeyValue, CanSKeyValue, StorageDeserializable}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.{Vertex, GraphUtil, IndexEdge, QueryParam}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author Junki Kim (wishoping@gmail.com), Hyunsung Jo (hyunsung.jo@gmail.com) on 1/7/16.
 */
class RedisIndexEdgeDeserializable extends StorageDeserializable[IndexEdge] {
  import StorageDeserializable._

  type QualifierRaw = (Array[(Byte, InnerValLike)],  // Index property key/value map
    VertexId, // target vertex id
    Byte,  // Operation code
    Boolean, // Whether or not target vertex id exists in Qualifier
    Long, // timestamp
    Int) // length of bytes read
  type ValueRaw = (Array[(Byte, InnerValLike)], Int)

  private def parseDegreeQualifier(kv: SKeyValue, version: String): QualifierRaw = {
    // skip first two length bytes(because it will be zero)
    var pos = 2
    val (tsInnerVal, numLenBytesRead) = InnerVal.fromBytes(kv.value, pos, 0, version)
    val ts = tsInnerVal.value.toString.toLong
    pos += numLenBytesRead
    val (degreeInnerVal, numValueBytesRead) = InnerVal.fromBytes(kv.value, pos, 0, version)
    val degree = degreeInnerVal.value.toString.toLong
    pos += numValueBytesRead

    val idxPropsRaw = Array(LabelMeta.degreeSeq -> InnerVal.withLong(degree, version))
    val tgtVertexIdRaw = VertexId(HBaseType.DEFAULT_COL_ID, InnerVal.withStr("0", version))

    (idxPropsRaw, tgtVertexIdRaw, GraphUtil.operations("insert"), false, ts, pos)
  }

  /**
   * Parse qualifier
   *
   *  byte map
   *  [ qualifier length byte | indexed property count byte | indexed property values bytes | timestamp bytes | target id bytes | operation code byte ]
   *
   *  - Please refer to https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/util/OrderedBytes.html regarding actual byte size of InnerVals.
   *
   * @param kv
   * @param totalQualifierLen
   * @param version
   * @return
   */
  private def parseQualifier(kv: SKeyValue, totalQualifierLen: Int, version: String): QualifierRaw = {
    var qualifierLen = 0
    // skip first byte: qualifier length
    var pos = 1
    val (idxPropsRaw, tgtVertexIdRaw, tgtVertexIdLen, timestamp) = {
      val (props, endAt) = bytesToProps(kv.value, pos, version)
      pos = endAt
      qualifierLen += endAt

      // get timestamp value
      val (tsInnerVal, numOfBytesUsed) = InnerVal.fromBytes(kv.value, pos, 0, version)
      val ts = tsInnerVal.value.toString.toLong
      pos += numOfBytesUsed
      qualifierLen += numOfBytesUsed

      val (tgtVertexId, tgtVertexIdLen) =
        if (pos == totalQualifierLen) { // target id is included in qualifier
          (HBaseType.defaultTgtVertexId, 0)
        } else {
          TargetVertexId.fromBytes(kv.value, pos, kv.value.length, version)
        }
      pos += tgtVertexIdLen
      qualifierLen += tgtVertexIdLen
      (props, tgtVertexId, tgtVertexIdLen, ts)
    }
    val op = kv.value(totalQualifierLen)
    pos += 1

    (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdLen != 0, timestamp, pos)
  }

  /** version 1 and 2 share same code for row key parsing */
  def parseRow(kv: SKeyValue, version: String): RowKeyRaw = {
    var pos = 0
    // add dummy data in place of hash bytes (Redis' row key schema doesn't have leading hash bytes(2))
    val row = Bytes.add(Array.fill[Byte](GraphUtil.bytesForMurMurHash)(0), kv.row)
    val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(row, pos, row.length, version)
    pos += srcIdLen
    val labelWithDir = LabelWithDirection(Bytes.toInt(row, pos, 4))
    pos += 4
    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(row, pos)

    val rowLen = srcIdLen + 4 + 1
    (srcVertexId, labelWithDir, labelIdxSeq, isInverted, rowLen)
  }

  private def parseValue(kv: SKeyValue, offset: Int, version: String): ValueRaw = {
    val (props, endAt) = bytesToKeyValues(kv.value, offset, 0, version)
    (props, endAt)
  }

  private def parseDegreeValue(kv: SKeyValue, offset: Int, version: String): ValueRaw = {
    (Array.empty[(Byte, InnerValLike)], 0)
  }

  /**
   * Read first byte as qualifier length and check if length equals 0
   * @param kv
   * @return true if first byte is zero value
   */
  private def isEmptyQualifier(kv: SKeyValue) = Array.fill[Byte](1)(kv.value(0)) == Bytes.toBytes(0.toByte)

  def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[IndexEdge]): IndexEdge = {
    assert(_kvs.size == 1)

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }
    val kv = kvs.head

    val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
    }.getOrElse(parseRow(kv, version))

    val totalQualifierLen = kv.value(0).toInt

    val (idxPropsRaw, tgtVertexIdRaw, op, isTgtVertexIdInQualifier, timestamp, numBytesRead) =
      if (isEmptyQualifier(kv)) parseDegreeQualifier(kv, version) // degree edge case
      else parseQualifier(kv, totalQualifierLen, version)

    // get non-indexed property key/ value
    val (props, _) = if (op == GraphUtil.operations("incrementCount")) {
      val (amount, _) = InnerVal.fromBytes(kv.value, numBytesRead, 0, version)
      val countVal = amount.value.toString.toLong
      val dummyProps = Array(LabelMeta.countSeq -> InnerVal.withLong(countVal, version))
      (dummyProps, 8)
    } else if (isEmptyQualifier(kv)) {
      // degree is already included in qualifier, so just pass empty value
      parseDegreeValue(kv, numBytesRead, version)
    } else {
      // non-indexed property key/ value retrieval

      parseValue(kv, numBytesRead, version)
    }

    val index = queryParam.label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException("invalid index seq"))

    val idxProps = for {
      (seq, (k, v)) <- index.metaSeqs.zip(idxPropsRaw)
    } yield {
        if (k == LabelMeta.degreeSeq) k -> v
        else seq -> v
      }

    val idxPropsMap = idxProps.toMap
    val tgtVertexId = if (isTgtVertexIdInQualifier) {
      idxPropsMap.get(LabelMeta.toSeq) match {
        case None => tgtVertexIdRaw
        case Some(vId) => TargetVertexId(HBaseType.DEFAULT_COL_ID, vId)
      }
    } else tgtVertexIdRaw

    val _mergedProps = (idxProps ++ props).toMap
    val mergedProps =
      if (_mergedProps.contains(LabelMeta.timeStampSeq)) _mergedProps
      else _mergedProps + (LabelMeta.timeStampSeq -> InnerVal.withLong(timestamp, version))

    IndexEdge(Vertex(srcVertexId, timestamp), Vertex(tgtVertexId, timestamp), labelWithDir, op, timestamp, labelIdxSeq, mergedProps)
  }
}
