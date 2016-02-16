package com.kakao.s2graph.core.storage.rocks

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.cache.{CacheBuilder, Cache}
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.hbase._
import com.kakao.s2graph.core.storage.{SKeyValue, StorageDeserializable, StorageSerializable, Storage}
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
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

  try {
    // a factory method that returns a RocksDB instance
    // only for testing now.

    db = RocksDB.open(options, "/tmp/rocks")

  } catch {
    case e: RocksDBException =>
      logger.error(s"initialize rocks db storage failed.", e)
  }

  val lockMap = CacheBuilder.newBuilder()
    .concurrencyLevel(16)
    .expireAfterAccess(1000 * 60, TimeUnit.MILLISECONDS)
    .expireAfterWrite(1000 * 60, TimeUnit.MILLISECONDS)
    .maximumSize(10000000)
    .build[String, Integer]()

  def withLock[A](key: Array[Byte])(op: => A): A = {
    val lock = new Integer(-1)
    val lockKey = Bytes.toString(key)

    lockMap.asMap().putIfAbsent(lockKey, lock) match {
      case null => lock.synchronized(op)
      case obj => obj.synchronized(op)
    }
  }

  /** Mutation Logics */
  override def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val ret =
        try {
          elementRpcs.foreach { rpc =>
            withLock(rpc.row) {
              rpc.operation match {
                case SKeyValue.Put => db.put(rpc.row, rpc.value)
                case SKeyValue.Delete => db.remove(rpc.row)
                case SKeyValue.Increment =>
                  val javaLong = Bytes.toLong(rpc.value)
                  db.merge(rpc.row, longToBytes(javaLong))
                case _ => throw new RuntimeException(s"not supported rpc operation. ${rpc.operation}")
              }
            }
          }
          true
        } catch {
          case e: Exception => false
        }

      Future.successful(ret)
    }
  }

  override def writeToStorage(rpc: SKeyValue, withWait: Boolean): Future[Boolean] = {
    val ret = try {
      try {
        withLock(rpc.row) {
          rpc.operation match {
            case SKeyValue.Put => db.put(writeOptions, rpc.row, rpc.value)
            case SKeyValue.Delete => db.remove(writeOptions, rpc.row)
            case SKeyValue.Increment =>
              val javaLong = Bytes.toLong(rpc.value)
              db.merge(writeOptions, rpc.row, longToBytes(javaLong))
            case _ => throw new RuntimeException(s"not supported rpc operation. ${rpc.operation}")
          }
        }
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

  override def fetchKeyValues(startStopKeyRange: AnyRef): Future[Seq[SKeyValue]] = {
    startStopKeyRange match {
      case (startKey: Array[Byte], stopKey: Array[Byte]) =>
        def op = {
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
          Future.successful(kvs.toSeq)
        }

        // sync only snapshot update
        if (Bytes.compareTo(startKey, stopKey) == 0) withLock(startKey)(op)
        else op

      case _ => Future.successful(Seq.empty)
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

  override def writeLock(rpc: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean] = {
    withLock(rpc.row) {
      try {
        val fetchedValue = db.get(rpc.row)
        val innerRet = expectedOpt match {
          case None =>
            if (fetchedValue == null) {
              db.put(writeOptions, rpc.row, rpc.value)
              true
            } else {
              false
            }
          case Some(kv) =>
            if (fetchedValue == null) {
              false
            } else {
              if (Bytes.compareTo(fetchedValue, kv.value) == 0) {
                db.put(writeOptions, rpc.row, rpc.value)
                true
              } else {
                false
              }
            }
        }

        Future.successful(innerRet)
      } catch {
        case e: Exception => Future.successful(false)
      }
    }
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
