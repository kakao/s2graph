package com.kakao.s2graph.core.storage.rocks

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.Cache
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.hbase._
import com.kakao.s2graph.core.storage.{SKeyValue, StorageDeserializable, StorageSerializable, Storage}
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.rocksdb._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, ExecutionContext}

object RocksDBHelper {
  def longToBytes(value: Long): Array[Byte] = {
    val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder())
    longBuffer.clear()
    longBuffer.putLong(value)
    longBuffer.array()
  }
  def bytesToLong(data: Array[Byte], offset: Int): Long = {
    if (data != null) {
      val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder())
      longBuffer.put(data, offset, 8)
      longBuffer.flip()
      longBuffer.getLong()
    } else 0L
  }
}
class RocksDBStorage(override val config: Config)(implicit ec: ExecutionContext)
  extends Storage[SKeyValue, Future[QueryRequestWithResult]](config) {

  import RocksDBHelper._
  import HSerializable._

  override val indexEdgeDeserializer = new IndexEdgeDeserializable(bytesToLong)
  val emptyBytes = Array.empty[Byte]
  val table = Array.empty[Byte]
  val qualifier = Array.empty[Byte]

  RocksDB.loadLibrary()

  val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(100)
    .setMergeOperatorName("uint64add")

  var db: RocksDB = null

  try {
    // a factory method that returns a RocksDB instance
    db = RocksDB.open(options, "/tmp/rocks")

  } catch {
    case e: RocksDBException =>
      logger.error(s"initialize rocks db storage failed.", e)
  }

  val rowKeyLocks = new mutable.HashMap[Seq[Byte], AtomicBoolean]

  /** Mutation Logics */
  override def writeToStorage(rpc: SKeyValue, withWait: Boolean): Future[Boolean] = Future {
    rpc.operation match {
      case SKeyValue.Put => db.put(rpc.row, rpc.value)
      case SKeyValue.Delete => db.remove(rpc.row)
      case SKeyValue.Increment =>
        val javaLong = Bytes.toLong(rpc.value)
        db.merge(rpc.row, longToBytes(javaLong))
      case _ => throw new RuntimeException(s"not supported rpc operation. ${rpc.operation}")
    }
    true
  }

  override def createTable(zkAddr: String,
                           tableName: String,
                           cfs: List[String],
                           regionMultiplier: Int,
                           ttl: Option[Int],
                           compressionAlgorithm: String): Unit = {
    // nothing to do for now.
  }

  /** Query Logic */
  val HardLimit = 10000
  override def fetchKeyValues(startStopKeyRange: AnyRef): Future[Seq[SKeyValue]] = Future {
    startStopKeyRange match {
      case (startKey: Array[Byte], stopKey: Array[Byte]) =>
        val iter = db.newIterator()
        var idx = 0
        iter.seek(startKey)

        val kvs = new ListBuffer[SKeyValue]()
        val ts = System.currentTimeMillis()
        iter.seek(startKey)
        while (iter.isValid && Bytes.compareTo(iter.key, stopKey) <= 0 && idx < HardLimit) {
          kvs += SKeyValue(table, iter.key, edgeCf, qualifier, iter.value, System.currentTimeMillis())
          iter.next()
          idx += 1
        }
        kvs.toSeq
      case _ => Seq.empty
    }
  }

  override def buildRequest(queryRequest: QueryRequest): (Array[Byte], Array[Byte]) = {
    queryRequest.queryParam.tgtVertexInnerIdOpt match {
      case None => // indexEdges
        val queryParam = queryRequest.queryParam
        val edge = toRequestEdge(queryRequest)
        val indexEdgeOpt = edge.edgesWithIndex.filter(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq).headOption
        val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))
        val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
        val labelWithDirBytes = indexEdge.labelWithDir.bytes
        val labelIndexSeqWithIsInvertedBytes = StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

        val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, Bytes.add(labelIndexSeqWithIsInvertedBytes, Array.fill(1)(edge.op)))
        val (startKey, stopKey) =
          if (queryParam.columnRangeFilter != null) {
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => Bytes.add(baseKey, queryParam.columnRangeFilterMinBytes)
            }
            (_startKey, Bytes.add(baseKey, queryParam.columnRangeFilterMaxBytes))
          } else {
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => baseKey
            }
            (_startKey, Bytes.add(baseKey, Array.fill(1)(-1)))
          }
        (startKey, stopKey)
      case Some(tgtId) => // snapshotEdge
        val kv = snapshotEdgeSerializer(toRequestEdge(queryRequest).toSnapshotEdge).toKeyValues.head
        (kv.row, kv.row)
    }
  }


  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = {

    fetchKeyValues(buildRequest(queryRequest)) map { kvs =>
      val queryParam = queryRequest.queryParam
      val edgeWithScores = toEdges(kvs, queryParam, prevStepScore, isInnerCall, parentEdges)
//
//      val filteredEdgeWithScores = queryParam.duration match {
//        case None => edgeWithScores
//        case Some((minTs, maxTs)) => edgeWithScores.filter { edgeWithScore =>
//          edgeWithScore.edge.isDegree || (edgeWithScore.edge.ts >= minTs && edgeWithScore.edge.ts < maxTs)
//        }
//      }

      val resultEdgesWithScores =
        if (queryRequest.queryParam.sample >= 0) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
        else edgeWithScores

      QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.lastOption.map(_.row).getOrElse(Array.empty)))
    }
  }


  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = {
    val futures = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
      parentEdges <- prevStepEdges.get(queryRequest.vertex.id)
    } yield fetch(queryRequest, prevStepScore, isInnerCall = false, parentEdges)

    Future.sequence(futures)
  }

  val lockMap = new java.util.concurrent.ConcurrentHashMap[String, AtomicBoolean]()

  override def writeLock(rpc: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean] = {
    val lockKey = Bytes.toString(rpc.row)
    db.synchronized {
      val ret = lockMap.putIfAbsent(lockKey, new AtomicBoolean(true)) match {
        case null =>
          val fetchedValue = db.get(rpc.row)
          val innerRet = expectedOpt match {
            case None =>
              if (fetchedValue == null) {
                db.put(rpc.row, rpc.value)
                true
              } else {
                false
              }
            case Some(kv) =>
              if (fetchedValue == null) {
                false
              } else {
                if (Bytes.compareTo(fetchedValue, kv.value) == 0) {
                  db.put(rpc.row, rpc.value)
                  true
                } else {
                  false
                }
              }
          }

          lockMap.remove(lockKey)
          innerRet
        case _ => false
      }

      Future.successful(ret)
    }
  }

//  override def commitProcess(edge: Edge, statusCode: Byte)
//                            (snapshotEdgeOpt: Option[Edge], kvOpt: Option[SKeyValue])
//                            (lockEdge: SnapshotEdge, releaseLockEdge: SnapshotEdge, _edgeMutate: EdgeMutate): Future[Boolean] = {
//
//    val writeBatch = new WriteBatch()
//    val lockEdgePut = buildPutAsync(lockEdge).head
//
//    val indexEdgeMutations = indexedEdgeMutations(_edgeMutate)
//    val incrementMutations = increment(increments(_edgeMutate))
//    val releaseLockEdgePut = buildPutAsync(releaseLockEdge).head
//
//    writeBatch.put(lockEdgePut.row, lockEdgePut.value)
//    indexEdgeMutations.foreach { kv =>
//      kv.operation match {
//        case SKeyValue.Put => writeBatch.put(kv.row, kv.value)
//        case SKeyValue.Delete => writeBatch.remove(kv.row)
//        case _ => throw new RuntimeException(s"not supported rpc operation. ${kv.operation}")
//      }
//    }
//    incrementMutations.foreach { kv =>
//      val newValue = Bytes.toLong(kv.value) match {
//        case 1L => longToBytes(1L)
//        case -1L => longToBytes(-1L)
//        case _ => throw new RuntimeException("!!!!!!")
//      }
//      writeBatch.merge(kv.row, newValue)
//    }
//    writeBatch.put(releaseLockEdgePut.row, releaseLockEdgePut.value)
//    val ret =
//      try {
//        val writeOption = new WriteOptions()
//        db.write(writeOption, writeBatch)
//        true
//      } catch {
//        case e: RocksDBException =>
//          logger.error(s"CommitProcess failed.", e)
//          false
//      }
//    Future.successful(ret)
//  }




  /** Build backend storage specific RPC */
  override def put(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs.map { kv => kv.copy(operation = SKeyValue.Put)}

  override def increment(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs.map { kv => kv.copy(operation = SKeyValue.Increment) }

  override def delete(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs.map { kv => kv.copy(operation = SKeyValue.Delete)}


  /** Management Logic */
  override def flush(): Unit = db.close()

  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    Future.successful(Seq.empty)
  }

  /** End of Mutation */

}
