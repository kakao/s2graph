package com.kakao.s2graph.core.storage.rocks

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.Cache
import com.kakao.s2graph.core.GraphExceptions.{SnapshotEdgeLockedException, FetchTimeoutException}
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.hbase._
import com.kakao.s2graph.core.storage._
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{PutRequest, KeyValue, GetRequest}
import org.rocksdb._

import scala.collection.{Seq, mutable}
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
  extends Storage[Future[QueryRequestWithResult]](config) {

  import RocksDBHelper._
  import HSerializable._

  override val indexEdgeDeserializer = new IndexEdgeDeserializable(bytesToLong)
  val emptyBytes = Array.empty[Byte]
  val table = Array.empty[Byte]
  val qualifier = Array.empty[Byte]

  RocksDB.loadLibrary()

  val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(0)
    .setMergeOperatorName("uint64add")


  var db: RocksDB = null
  val writeOptions = new WriteOptions().setSync(true)

  val client = AsynchbaseStorage.makeClient(config)

  try {
    // a factory method that returns a RocksDB instance
    // only for testing now.

    db = RocksDB.open(options, "/tmp/rocks")

  } catch {
    case e: RocksDBException =>
      logger.error(s"initialize rocks db storage failed.", e)
  }

  val rowKeyLocks = new mutable.HashMap[Seq[Byte], AtomicBoolean]

  /** Mutation Logics */
  override def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val ret =
        try {
          val writeBatch = new WriteBatch()
          elementRpcs.map { rpc =>
            rpc.operation match {
              case SKeyValue.Put => writeBatch.put(rpc.row, rpc.value)
              case SKeyValue.Delete => writeBatch.remove(rpc.row)
              case SKeyValue.Increment =>
                val javaLong = Bytes.toLong(rpc.value)
                writeBatch.merge(rpc.row, longToBytes(javaLong))
              case _ => throw new RuntimeException(s"not supported rpc operation. ${rpc.operation}")
            }
          }
          db.write(writeOptions, writeBatch)
          true
        } catch {
          case e: Exception =>
            false
        }


      Future.successful(ret)
    }
  }
  override def writeToStorage(rpc: SKeyValue, withWait: Boolean): Future[Boolean] = {

    val ret = try {
      rpc.operation match {
        case SKeyValue.Put => db.put(writeOptions, rpc.row, rpc.value)
        case SKeyValue.Delete => db.remove(writeOptions, rpc.row)
        case SKeyValue.Increment =>
          val javaLong = Bytes.toLong(rpc.value)
          db.merge(writeOptions, rpc.row, longToBytes(javaLong))
        case _ => throw new RuntimeException(s"not supported rpc operation. ${rpc.operation}")
      }
      true
    } catch {
      case e: Exception =>
        false
    }
    Future.successful(ret)
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
  override def fetchSnapshotEdgeKeyValues(queryParamWithStartStopKeyRange: AnyRef): Future[Seq[SKeyValue]] = {
    import Extensions.DeferOps
    import collection.JavaConversions._
    queryParamWithStartStopKeyRange match {
      case (queryParam: QueryParam, (startKey: Array[Byte], stopKey: Array[Byte])) =>
        val label = queryParam.label
        val get = new GetRequest(label.hbaseTableName.getBytes, startKey, edgeCf, Array.empty[Byte])
        client.get(get).toFuture.map { kvs =>
          kvs.map { kv => implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv) }.toSeq
        }
//        val lockKey = Bytes.toString(startKey)
//        val lock = lockMap.getOrDefault(lockKey, new AtomicBoolean(false))
//        if (lock.getAndSet(true)) {
//          Future.failed(new SnapshotEdgeLockedException("read for fetchSnapshotEdgeKeyValues is locked."))
//        } else {
//          try {
//            val fetched = db.get(startKey)
//            val ret =
//              if (fetched == null) Seq.empty
//              else {
//                Seq(
//                  SKeyValue(table, startKey, edgeCf, qualifier, fetched, System.currentTimeMillis())
//                )
//              }
//            Future.successful(ret)
//          } finally {
//            lockMap.remove(lock)
//          }
//        }
      case _ => Future.failed(new RuntimeException("wrong class type for startStopKeyRange on fetchSnapshotEdgeKeyValues"))
    }
  }
  override def fetchIndexEdgeKeyValues(queryParamWithStartStopKeyRange: AnyRef): Future[Seq[SKeyValue]] =  {

    val ret = queryParamWithStartStopKeyRange match {
      case (queryParam: QueryParam, (startKey: Array[Byte], stopKey: Array[Byte])) =>
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
    Future.successful(ret)
  }

  override def buildRequest(queryRequest: QueryRequest): (QueryParam, (Array[Byte], Array[Byte])) = {
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
        (queryRequest.queryParam, (startKey, stopKey))
      case Some(tgtId) => // snapshotEdge
        val kv = snapshotEdgeSerializer(toRequestEdge(queryRequest).toSnapshotEdge).toKeyValues.head
        (queryRequest.queryParam, (kv.row, kv.row))
    }
  }


  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = {

    fetchIndexEdgeKeyValues(buildRequest(queryRequest)) map { kvs =>
      val queryParam = queryRequest.queryParam
      val edgeWithScores = toEdges(kvs, queryParam, prevStepScore, isInnerCall, parentEdges)

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
    import Extensions.DeferOps
    val put = new PutRequest(rpc.table, rpc.row, rpc.cf, rpc.qualifier, rpc.value, rpc.timestamp)
    client.compareAndSet(put, expectedOpt.map(_.value).getOrElse(Array.empty)).toFuture.map(_.booleanValue())
//    val lockKey = Bytes.toString(rpc.row)
//
////    db.synchronized {
//      val ret = lockMap.putIfAbsent(lockKey, new AtomicBoolean(true)) match {
//        case null =>
//          val fetchedValue = db.get(rpc.row)
//          val innerRet = expectedOpt match {
//            case None =>
//              if (fetchedValue == null) {
//                db.put(writeOptions, rpc.row, rpc.value)
//                true
//              } else {
//                false
//              }
//            case Some(kv) =>
//              if (fetchedValue == null) {
//                false
//              } else {
//                if (Bytes.compareTo(fetchedValue, kv.value) == 0) {
//                  db.put(writeOptions, rpc.row, rpc.value)
//                  true
//                } else {
//                  false
//                }
//              }
//          }
//
//          lockMap.remove(lockKey)
//          innerRet
//        case _ => false
//      }
//
//      Future.successful(ret)
////    }


  }


  /** Management Logic */
  override def flush(): Unit = {
    db.flush(new FlushOptions().setWaitForFlush(true))
    db.close()
  }

  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    Future.successful(Seq.empty)
  }

  /** End of Mutation */

}
