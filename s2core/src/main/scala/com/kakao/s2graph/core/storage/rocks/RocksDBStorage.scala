package com.kakao.s2graph.core.storage.rocks

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{ReentrantLock, ReadWriteLock}

import com.google.common.cache.{CacheLoader, CacheBuilder, Cache}
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
    .setWriteBufferSize(1024 * 1024 * 512)
    .setMergeOperatorName("uint64add")
    .setAllowOsBuffer(true)
    .setArenaBlockSize(1024 * 32)
    .createStatistics()
    .setDbLogDir("./rocks.log")
    .setMaxBackgroundCompactions(2)
    .setMaxBackgroundFlushes(2)


  var db: RocksDB = null
  val writeOptions = new WriteOptions()
  //    .setSync(true)

  try {
    // a factory method that returns a RocksDB instance
    // only for testing now.

    db = RocksDB.open(options, "/tmp/rocks")
  } catch {
    case e: RocksDBException =>
      logger.error(s"initialize rocks db storage failed.", e)
  }

  val cacheLoader = new CacheLoader[String, ReentrantLock] {
    override def load(key: String) = new ReentrantLock()
  }

  val lockMap = CacheBuilder.newBuilder()
    .concurrencyLevel(124)
    .expireAfterAccess(LockExpireDuration, TimeUnit.MILLISECONDS)
    .expireAfterWrite(LockExpireDuration, TimeUnit.MILLISECONDS)
    .maximumSize(1000 * 10 * 10 * 10 * 10)
    .build[String, ReentrantLock](cacheLoader)

  def withLock[A](key: Array[Byte])(op: => A): A = {
    val lockKey = Bytes.toString(key)
    val lock = lockMap.get(lockKey)

    try {
      lock.lock
      op
    } finally {
      lock.unlock()
    }
  }

  def buildWriteBatch(kvs: Seq[SKeyValue]): WriteBatch = {
    val writeBatch = new WriteBatch()
    kvs.foreach { kv =>
      kv.operation match {
        case SKeyValue.Put => writeBatch.put(kv.row, kv.value)
        case SKeyValue.Delete => writeBatch.remove(kv.row)
        case SKeyValue.Increment =>
          val javaLong = Bytes.toLong(kv.value)
          writeBatch.merge(kv.row, longToBytes(javaLong))
        case _ => throw new RuntimeException(s"not supported rpc operation. ${kv.operation}")
      }
    }
    writeBatch
  }

  override def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val ret = {
        val writeBatch = buildWriteBatch(elementRpcs)
        try {
          db.write(writeOptions, writeBatch)
          true
        } catch {
          case e: Exception =>
            logger.error(s"writeAsyncSimple failed.", e)
            false
        } finally {
          writeBatch.dispose()
        }
      }
      Future.successful(ret)
    }
  }

  /**
    * originally storage should implement this since this will be called by writeAsyncSimple
    */
  override def writeToStorage(rpc: SKeyValue, withWait: Boolean): Future[Boolean] =
    Future.successful(true)

  override def createTable(zkAddr: String,
                           tableName: String,
                           cfs: List[String],
                           regionMultiplier: Int,
                           ttl: Option[Int],
                           compressionAlgorithm: String): Unit = {
    // nothing to do for now.
  }

  override def fetchSnapshotEdgeKeyValues(queryParamWithStartStopKeyRange: AnyRef): Future[Seq[SKeyValue]] = {
    queryParamWithStartStopKeyRange match {
      case (queryParam: QueryParam, (startKey: Array[Byte], stopKey: Array[Byte])) =>
        def op = {
          try {
            val v = db.get(startKey)
            val kvs =
              if (v == null) Seq.empty
              else Seq(SKeyValue(table, startKey, edgeCf, qualifier, v, System.currentTimeMillis()))

            Future.successful(kvs)
          } catch {
            case e: RocksDBException =>
              logger.error("Fetch snapshotEdge failed", e)
              Future.failed(new RuntimeException("Fetch snapshotEdge failed"))
          }
        }

        withLock(startKey)(op)

      case _ => Future.successful(Seq.empty)
    }
  }

  def fetchIndexEdgeKeyValues(queryParamWithStartStopKeyRange: AnyRef): Future[Seq[SKeyValue]] = {
    queryParamWithStartStopKeyRange match {
      case (queryParam: QueryParam, (startKey: Array[Byte], stopKey: Array[Byte])) =>
        def op = {
          val iter = db.newIterator()
          try {
            var idx = 0
            iter.seek(startKey)

            val kvs = new ListBuffer[SKeyValue]()
            val ts = System.currentTimeMillis()
            iter.seek(startKey)
            while (iter.isValid && Bytes.compareTo(iter.key, stopKey) <= 0 && idx < queryParam.limit) {
              kvs += SKeyValue(table, iter.key, edgeCf, qualifier, iter.value, System.currentTimeMillis())
              iter.next()
              idx += 1
            }
            Future.successful(kvs.toSeq)
          } finally {
            iter.dispose()
          }
        }
        op
      case _ => Future.successful(Seq.empty)
    }
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

  override def writeLock(rpc: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean] = {
    def op = {
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
        case e: RocksDBException =>
          logger.error(s"Write lock failed", e)
          Future.successful(false)
      }
    }

    withLock(rpc.row)(op)
  }

  /** Management Logic */
  override def flush(): Unit = {
    db.flush(new FlushOptions().setWaitForFlush(true))
    db.close()
  }

  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    Future.successful(Seq.empty)
  }

  /**
   * fetch Vertex for given request from storage.
   * @param request
   * @return
   */
  override def fetchVertexKeyValues(request: AnyRef): Future[scala.Seq[SKeyValue]] = Future.successful(Seq.empty)
}
