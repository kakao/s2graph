package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.{CanSKeyValue, StorageDeserializable, SKeyValue}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.logger
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

class IndexEdgeDeserializable extends HDeserializable[IndexEdge] {

  import StorageDeserializable._

  type QualifierRaw = (Array[(Byte, InnerValLike)], VertexId, Byte, Boolean, Int)
  type ValueRaw = (Array[(Byte, InnerValLike)], Int)

  private def parseDegreeQualifier(kv: SKeyValue, version: String): QualifierRaw = {
    val degree = Bytes.toLong(kv.value)
    val idxPropsRaw = Array(LabelMeta.degreeSeq -> InnerVal.withLong(degree, version))
    val tgtVertexIdRaw = VertexId(HBaseType.DEFAULT_COL_ID, InnerVal.withStr("0", version))
    (idxPropsRaw, tgtVertexIdRaw, GraphUtil.operations("insert"), false, 0)
  }

  private def parseQualifier(kv: SKeyValue, version: String): QualifierRaw = {
    var qualifierLen = 0
    var pos = 0
    val (idxPropsRaw, idxPropsLen, tgtVertexIdRaw, tgtVertexIdLen) = {
      val (props, endAt) = bytesToProps(kv.qualifier, pos, version)
      pos = endAt
      qualifierLen += endAt
      val (tgtVertexId, tgtVertexIdLen) = if (endAt == kv.qualifier.length) {
        (HBaseType.defaultTgtVertexId, 0)
      } else {
        TargetVertexId.fromBytes(kv.qualifier, endAt, kv.qualifier.length, version)
      }
      qualifierLen += tgtVertexIdLen
      (props, endAt, tgtVertexId, tgtVertexIdLen)
    }
    val (op, opLen) =
      if (kv.qualifier.length == qualifierLen) (GraphUtil.defaultOpByte, 0)
      else (kv.qualifier(qualifierLen), 1)

    qualifierLen += opLen

    (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdLen != 0, qualifierLen)
  }

  private def parseValue(kv: SKeyValue, version: String): ValueRaw = {
    val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, version)
    (props, endAt)
  }

  private def parseDegreeValue(kv: SKeyValue, version: String): ValueRaw = {
    (Array.empty[(Byte, InnerValLike)], 0)
  }



  /** version 1 and version 2 is same logic */
  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, _kvs: Seq[T], version: String, cacheElementOpt: Option[IndexEdge] = None): IndexEdge = {
    assert(_kvs.size == 1)

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

    val kv = kvs.head
    val (srcVertexId, labelWithDir, labelIdxSeq, _, _) = cacheElementOpt.map { e =>
      (e.srcVertex.id, e.labelWithDir, e.labelIndexSeq, false, 0)
    }.getOrElse(parseRow(kv, version))

    val (idxPropsRaw, tgtVertexIdRaw, op, tgtVertexIdInQualifier, _) =
      if (kv.qualifier.isEmpty) parseDegreeQualifier(kv, version)
      else parseQualifier(kv, version)

    var tgtVertexId = tgtVertexIdRaw
    var tsOpt: Option[Long] = None
    val mergedProps = new mutable.HashMap[Byte, InnerValLike]()


    if (op == GraphUtil.operations("incrementCount")) {
      val countVal = Bytes.toLong(kv.value)
      mergedProps += (LabelMeta.countSeq -> InnerVal.withLong(countVal, version))
    } else if (kv.qualifier.isEmpty) {

    } else {
      val (props, _) = bytesToKeyValues(kv.value, 0, kv.value.length, version)
      props.foreach { case (k, v) =>
        if (k == LabelMeta.timeStampSeq) tsOpt = Option(v.toString().toLong)
        mergedProps += (k -> v)
      }
    }

    val index = queryParam.label.indicesMap.getOrElse(labelIdxSeq, throw new RuntimeException("invalid index seq"))


    //    assert(kv.qualifier.nonEmpty && index.metaSeqs.size == idxPropsRaw.size)

    for {
      (seq, (k, v)) <- index.metaSeqs.zip(idxPropsRaw)
    } {
        if (seq == LabelMeta.toSeq) tgtVertexId = TargetVertexId(HBaseType.DEFAULT_COL_ID, v)
        if (seq == LabelMeta.timeStampSeq) tsOpt = Option(v.toString().toLong)

        if (k == LabelMeta.degreeSeq) mergedProps += k -> v
        else mergedProps += seq -> v
      }

//    val idxPropsMap = idxProps.toMap
//    val tgtVertexId = if (tgtVertexIdInQualifier) {
//      idxPropsMap.get(LabelMeta.toSeq) match {
//        case None => tgtVertexIdRaw
//        case Some(vId) => TargetVertexId(HBaseType.DEFAULT_COL_ID, vId)
//      }
//    } else tgtVertexIdRaw

//    val _mergedProps = (idxProps ++ props).toMap
//    val mergedProps =
//      if (_mergedProps.contains(LabelMeta.timeStampSeq)) _mergedProps
//      else _mergedProps + (LabelMeta.timeStampSeq -> InnerVal.withLong(kv.timestamp, version))
//
////    logger.error(s"$mergedProps")
////    val ts = mergedProps(LabelMeta.timeStampSeq).toString().toLong
//
//    val ts = kv.timestamp
    val ts = tsOpt.getOrElse(kv.timestamp)
    mergedProps += LabelMeta.timeStampSeq -> InnerVal.withLong(ts, version)
    IndexEdge(Vertex(srcVertexId, ts), Vertex(tgtVertexId, ts), labelWithDir, op, ts, labelIdxSeq, mergedProps.toMap)
  }
}

