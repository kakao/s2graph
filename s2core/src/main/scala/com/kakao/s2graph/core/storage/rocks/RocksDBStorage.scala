package com.kakao.s2graph.core.storage.rocks

import java.util.Base64

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

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, ExecutionContext}


class RocksDBStorage(override val config: Config)(implicit ec: ExecutionContext)
  extends Storage[SKeyValue, Future[QueryRequestWithResult]](config) {

  val emptyBytes = Array.empty[Byte]
  val table = Array.empty[Byte]
//  val cf = Array.empty[Byte]
  val cf = HSerializable.edgeCf
  val qualifier = Array.empty[Byte]

  RocksDB.loadLibrary()

  val options = new Options().setCreateIfMissing(true)
  var db: RocksDB = null
//  var edgeCfHandler: ColumnFamilyHandle = null
//  var vertexCfHandler: ColumnFamilyHandle = null

  try {
    // a factory method that returns a RocksDB instance
    db = RocksDB.open(options, "/tmp/rocks")
//    edgeCfHandler = db.createColumnFamily(new ColumnFamilyDescriptor(HSerializable.edgeCf))
//    vertexCfHandler = db.createColumnFamily(new ColumnFamilyDescriptor(HSerializable.vertexCf))
    // do something
  } catch {
    case e: RocksDBException =>

  }

  /** Mutation Logics */
  override def writeToStorage(rpc: SKeyValue, withWait: Boolean): Future[Boolean] = Future {
//    val cf = Bytes.toString(rpc.cf)
//    val cfHandler = cf match {
//      case "e" => edgeCfHandler
//      case "v" => vertexCfHandler
//      case _ => throw new RuntimeException(s"not supported column family. $cf")
//    }
//    logger.debug(s"$rpc")
    rpc.operation match {
      case SKeyValue.Put => db.put(rpc.row, rpc.value)
      case SKeyValue.Delete => db.remove(rpc.row)
      case SKeyValue.Increment =>
//      case SKeyValue.Put => db.put(cfHandler, rpc.row, rpc.value)
//      case SKeyValue.Delete => db.remove(cfHandler, rpc.row)
//      case SKeyValue.Increment => db.merge(cfHandler, )
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
  override def fetchKeyValues(startStopKeyRange: AnyRef): Future[Seq[SKeyValue]] = Future {
    startStopKeyRange match {
      case (startKey: Array[Byte], stopKey: Array[Byte]) =>
        val iter = db.newIterator()
        var idx = 0
        iter.seek(startKey)
        val HardLimit = 10000
        val kvs = new ListBuffer[SKeyValue]()
        val ts = System.currentTimeMillis()
        while (iter.isValid && Bytes.compareTo(iter.key, stopKey) <= 0 && idx <= HardLimit) {
          kvs += SKeyValue(table, cf, iter.key(), qualifier, iter.value(), ts)
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
                     parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = Future {

    val queryParam = queryRequest.queryParam
//    val edge = toRequestEdge(queryRequest)
//    val indexEdgeOpt = edge.edgesWithIndex.filter(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq).headOption
//    val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))
//    val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
//    val labelWithDirBytes = indexEdge.labelWithDir.bytes
//    val labelIndexSeqWithIsInvertedBytes = StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

//    val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, Bytes.add(labelIndexSeqWithIsInvertedBytes, Array.fill(1)(edge.op)))
//    val (startKey, stopKey) = (baseKey, Bytes.add(baseKey, Array.fill(1)(-1)))
    val (startKey, stopKey) = buildRequest(queryRequest)
    val iter = db.newIterator()
    val kvs = new ListBuffer[SKeyValue]()
    var idx = 0
    iter.seek(startKey)
    while (iter.isValid && Bytes.compareTo(iter.key(), stopKey) <= 0 && idx < queryParam.limit) {
      kvs += SKeyValue(table, iter.key, cf, qualifier, iter.value, System.currentTimeMillis())
      iter.next()
      idx += 1
    }
    val edgeWithScores = toEdges(kvs.toSeq, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
    val resultEdgesWithScores =
      if (queryRequest.queryParam.sample >= 0 ) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
      else edgeWithScores

    QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.lastOption.map(_.row).getOrElse(Array.empty)))

//    fetchKeyValues(buildRequest(queryRequest)) map { kvs =>
//      val edgeWithScores = toEdges(kvs.toSeq, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
//      val resultEdgesWithScores =
//        if (queryRequest.queryParam.sample >= 0 ) sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
//        else edgeWithScores
//
//      QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.lastOption.map(_.row).getOrElse(Array.empty)))
//    }
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
    Future.successful(true)
  }

  override def commitProcess(edge: Edge, statusCode: Byte)
                            (snapshotEdgeOpt: Option[Edge], kvOpt: Option[SKeyValue])
                            (lockEdge: SnapshotEdge, releaseLockEdge: SnapshotEdge, _edgeMutate: EdgeMutate): Future[Boolean] = {
    val writeBatch = new WriteBatch()
    val lockEdgePut = buildPutAsync(lockEdge).head

    val indexEdgeMutations = indexedEdgeMutations(_edgeMutate)
//    val incrementMutations = increments(_edgeMutate)

    val releaseLockEdgePut = buildPutAsync(releaseLockEdge).head

    writeBatch.put(lockEdgePut.row, lockEdgePut.value)
    indexEdgeMutations.foreach { kv => writeBatch.put(kv.row, kv.value) }
    //    incrementMutations.foreach { kv => writeBatch.put(kv, row, kv.value)}
    writeBatch.put(releaseLockEdgePut.row, releaseLockEdgePut.value)
    Future {
      try {
        db.write(new WriteOptions(), writeBatch)
        true
      } catch {
        case e: RocksDBException => false
      }
    }
  }

  /** Build backend storage specific RPC */
  override def put(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs.map { kv => kv.copy(operation = SKeyValue.Put)}

  override def increment(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs.map { kv => kv.copy(operation = SKeyValue.Increment)}

  override def delete(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs.map { kv => kv.copy(operation = SKeyValue.Delete)}


  /** Management Logic */
  override def flush(): Unit = db.close()

  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    Future.successful(Seq.empty)
  }

  /** End of Mutation */

}
