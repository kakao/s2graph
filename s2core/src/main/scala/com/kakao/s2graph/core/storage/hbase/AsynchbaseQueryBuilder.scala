package com.kakao.s2graph.core.storage.hbase

import java.util
import java.util.Base64
import java.util.concurrent.{Executors, TimeUnit}
import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.GraphExceptions.FetchTimeoutException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.StorageSerializable._
import com.kakao.s2graph.core.storage.{StorageSerializable, CanSKeyValue, SKeyValue, QueryBuilder}
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.stumbleupon.async.Deferred
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{RowFilter, Scanner, KeyValue, GetRequest}
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.{Map, Seq}
import scala.util.Random
import scala.concurrent.{ExecutionContext, Future}

class AsynchbaseQueryBuilder(storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[GetRequest, Deferred[QueryRequestWithResult]](storage) {

  import Extensions.DeferOps

  val maxSize = storage.config.getInt("future.cache.max.size")
  val expreAfterWrite = storage.config.getInt("future.cache.expire.after.write")
  val expreAfterAccess = storage.config.getInt("future.cache.expire.after.access")

  val futureCache = CacheBuilder.newBuilder()
//  .recordStats()
  .initialCapacity(maxSize)
  .concurrencyLevel(Runtime.getRuntime.availableProcessors())
  .expireAfterWrite(expreAfterWrite, TimeUnit.MILLISECONDS)
  .expireAfterAccess(expreAfterAccess, TimeUnit.MILLISECONDS)
//  .weakKeys()
  .maximumSize(maxSize).build[java.lang.Long, (Long, Deferred[QueryRequestWithResult])]()

  val emptyKeyValues = new util.ArrayList[KeyValue]()
  //  val scheduleTime = 60L * 60
//  val scheduleTime = 60
//  val scheduler = Executors.newScheduledThreadPool(1)
//
//  scheduler.scheduleAtFixedRate(new Runnable(){
//    override def run() = {
//      logger.info(s"[FutureCache]: ${futureCache.stats()}")
//    }
//  }, scheduleTime, scheduleTime, TimeUnit.SECONDS)

  private def fetchKeyValues(hbaseRpc: AnyRef): Deferred[util.ArrayList[KeyValue]] = {
    hbaseRpc match {
      case getRequest: GetRequest => storage.client.get(getRequest)
      case scanner: Scanner =>
        scanner.nextRows().withCallback { kvsLs =>
          val ls = new util.ArrayList[KeyValue]
          if (kvsLs == null) {

          } else {
            kvsLs.foreach { kvs =>
              if (kvs != null) kvs.foreach { kv => ls.add(kv) }
              else {

              }
            }
          }
          scanner.close()
          ls
        }.recoverWith { ex =>
          logger.error(s"fetchKeyValues failed.", ex)
          scanner.close()
          emptyKeyValues
        }
      case _ => Deferred.fromError(new RuntimeException(s"fetchKeyValues failed. $hbaseRpc"))
    }
  }
  override def fetchSnapshotEdge(edge: Edge): Future[(QueryParam, Option[Edge], Option[SKeyValue])] = {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(edge.tgtVertex.innerId))
    val q = Query.toQuery(Seq(edge.srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, edge.srcVertex, _queryParam)


    fetchKeyValues(buildRequest(queryRequest)) withCallback { kvs =>
      val (edgeOpt, kvOpt) =
        if (kvs.isEmpty()) (None, None)
        else {
          val _edgeOpt = storage.toEdges(kvs, queryParam, 1.0, isInnerCall = true, parentEdges = Nil).headOption.map(_.edge)
          val _kvOpt = kvs.headOption.map(kv => implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv))
          (_edgeOpt, _kvOpt)
        }
      (queryParam, edgeOpt, kvOpt)
    } recoverWith { ex =>
      logger.error(s"fetchQueryParam failed. fallback return.", ex)
      throw new FetchTimeoutException(s"${edge.toLogString}")
    } toFuture
  }

//  override def buildRequest(queryRequest: QueryRequest): GetRequest = {
  private def buildRequest(queryRequest: QueryRequest): AnyRef = {
    buildRequestInner(queryRequest)
  }

  private def buildRequestInner(queryRequest: QueryRequest): AnyRef = {
    val srcVertex = queryRequest.vertex
    //    val tgtVertexOpt = queryRequest.tgtVertexOpt
    val edgeCf = HSerializable.edgeCf
    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /** we use toSnapshotEdge so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertexId, tgtColumn.columnType, label.schemaVersion)
        (src, tgt)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        (src, src)
    }

    val (srcVId, tgtVId) = (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val currentTs = System.currentTimeMillis()
    val propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs)).toMap
    val edge = Edge(srcV, tgtV, labelWithDir, propsWithTs = propsWithTs)

    val kv = if (tgtVertexIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues.head
//      new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      storage.indexEdgeSerializer(indexedEdge).toKeyValues.head
//      val kv = storage.indexEdgeSerializer(indexedEdge).toKeyValues.head
//      val table = label.hbaseTableName.getBytes
//      val rowKey = kv.row
//      val cf = edgeCf
//      new GetRequest(table, rowKey, cf)
    }

    val (minTs, maxTs) = queryParam.duration.getOrElse((0L, Long.MaxValue))

    label.schemaVersion match {
      case HBaseType.VERSION4 if tgtVertexIdOpt.isEmpty =>
        val scanner = storage.client.newScanner(label.hbaseTableName.getBytes)
        scanner.setFamily(edgeCf)

        /**
         * TODO: remove this part.
         */
        val indexEdgeOpt = edge.edgesWithIndex.filter(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq).headOption
        val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))
//        val idxPropsMap = indexEdge.orders.toMap
//        val idxPropsBytes = propsToBytes(indexEdge.orders)

        val srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
        val labelWithDirBytes = indexEdge.labelWithDir.bytes
        val labelIndexSeqWithIsInvertedBytes = StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)
//        val labelIndexSeqWithIsInvertedStopBytes =  StorageSerializable.labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = true)
        val baseKey = Bytes.add(srcIdBytes, labelWithDirBytes, Bytes.add(labelIndexSeqWithIsInvertedBytes, Array.fill(1)(edge.op)))
        val (startKey, stopKey) =
          if (queryParam.columnRangeFilter != null) {
            // interval is set.
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => Bytes.add(baseKey, queryParam.columnRangeFilterMinBytes)
            }
            (_startKey, Bytes.add(baseKey, queryParam.columnRangeFilterMaxBytes))
          } else {
            /**
             * note: since propsToBytes encode size of property map at first byte, we are sure about max value here
             */
            val _startKey = queryParam.cursorOpt match {
              case Some(cursor) => Bytes.add(Base64.getDecoder.decode(cursor), Array.fill(1)(0))
              case None => baseKey
            }
            (_startKey, Bytes.add(baseKey, Array.fill(1)(-1)))
          }
//        logger.debug(s"[StartKey]: ${startKey.toList}")
//        logger.debug(s"[StopKey]: ${stopKey.toList}")

        scanner.setStartKey(startKey)
        scanner.setStopKey(stopKey)

        if (queryParam.limit == Int.MinValue) logger.debug(s"MinValue: $queryParam")

        scanner.setMaxVersions(1)
        scanner.setMaxNumRows(queryParam.limit)
        scanner.setMaxTimestamp(maxTs)
        scanner.setMinTimestamp(minTs)
        scanner.setRpcTimeout(queryParam.rpcTimeoutInMillis)
        // SET option for this rpc properly.
        scanner
      case _ =>
        val get =
          if (tgtVertexIdOpt.isDefined) new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
          else new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf)

        get.maxVersions(1)
        get.setFailfast(true)
        get.setMaxResultsPerColumnFamily(queryParam.limit)
        get.setRowOffsetPerColumnFamily(queryParam.offset)
        get.setMinTimestamp(minTs)
        get.setMaxTimestamp(maxTs)
        get.setTimeout(queryParam.rpcTimeoutInMillis)

        if (queryParam.columnRangeFilter != null) get.setFilter(queryParam.columnRangeFilter)

        get
    }
  }

  override def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Deferred[QueryRequestWithResult] = {
    //TODO:
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(tgtVertex.innerId))
    val q = Query.toQuery(Seq(srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, srcVertex, _queryParam)
    fetch(queryRequest, 1.0, isInnerCall = true, parentEdges = Nil)
  }

  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Deferred[QueryRequestWithResult] = {
    @tailrec
    def randomInt(sampleNumber: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
      if (range < sampleNumber || set.size == sampleNumber) set
      else randomInt(sampleNumber, range, set + Random.nextInt(range))
    }

    def sample(edges: Seq[EdgeWithScore], n: Int): Seq[EdgeWithScore] = {
      if (edges.size <= n){
        edges
      }else{
        val plainEdges = if (queryRequest.queryParam.offset == 0) {
          edges.tail
        } else edges

        val randoms = randomInt(n, plainEdges.size)
        var samples = List.empty[EdgeWithScore]
        var idx = 0
        plainEdges.foreach { e =>
          if (randoms.contains(idx)) samples = e :: samples
          idx += 1
        }
        samples.toSeq
      }

    }

    def fetchInner(hbaseRpc: AnyRef) = {
      fetchKeyValues(hbaseRpc) withCallback { kvs =>
        val edgeWithScores = storage.toEdges(kvs.toSeq, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
        val resultEdgesWithScores = if (queryRequest.queryParam.sample >= 0 ) {
          sample(edgeWithScores, queryRequest.queryParam.sample)
        } else edgeWithScores
        QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.last.key()))
      } recoverWith { ex =>
        logger.error(s"fetchQueryParam failed. fallback return.", ex)
        QueryRequestWithResult(queryRequest, QueryResult(isFailure = true))
      }
    }
    def checkAndExpire(hbaseRpc: AnyRef,
                       cacheKey: Long,
                       cacheTTL: Long,
                       cachedAt: Long,
                       defer: Deferred[QueryRequestWithResult]): Deferred[QueryRequestWithResult] = {
      if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
        // future is too old. so need to expire and fetch new data from storage.
        futureCache.asMap().remove(cacheKey)
        val newPromise = new Deferred[QueryRequestWithResult]()
        futureCache.asMap().putIfAbsent(cacheKey, (System.currentTimeMillis(), newPromise)) match {
          case null =>
            // only one thread succeed to come here concurrently
            // initiate fetch to storage then add callback on complete to finish promise.
            fetchInner(hbaseRpc) withCallback { queryRequestWithResult =>
              newPromise.callback(queryRequestWithResult)
              queryRequestWithResult
            }
            newPromise
          case (cachedAt, oldDefer) => oldDefer
        }
      } else {
        // future is not to old so reuse it.
        defer
      }
    }

    val queryParam = queryRequest.queryParam
    val cacheTTL = queryParam.cacheTTLInMillis
    val request = buildRequest(queryRequest)

    if (cacheTTL <= 0) fetchInner(request)
    else {
      val cacheKeyBytes = Bytes.add(queryRequest.query.cacheKeyBytes, toCacheKeyBytes(request))
      val cacheKey = queryParam.toCacheKey(cacheKeyBytes)

      val cacheVal = futureCache.getIfPresent(cacheKey)
      cacheVal match {
        case null =>
          // here there is no promise set up for this cacheKey so we need to set promise on future cache.
          val promise = new Deferred[QueryRequestWithResult]()
          val now = System.currentTimeMillis()
          val (cachedAt, defer) = futureCache.asMap().putIfAbsent(cacheKey, (now, promise)) match {
            case null =>
              fetchInner(request) withCallback { queryRequestWithResult =>
                promise.callback(queryRequestWithResult)
                queryRequestWithResult
              }
              (now, promise)
            case oldVal => oldVal
          }
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
        case (cachedAt, defer) =>
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
      }
    }
  }


//  override def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = {
  def toCacheKeyBytes(hbaseRpc: AnyRef): Array[Byte] = {
    hbaseRpc match {
      case getRequest: GetRequest => getRequest.key()
      case scanner: Scanner => scanner.getCurrentKey()
      case _ =>
        logger.error(s"toCacheKeyBytes failed. not supported class type. $hbaseRpc")
        Array.empty[Byte]
    }
//
//    Option(getRequest.family()).foreach(family => bytes = Bytes.add(bytes, family))
//    Option(getRequest.qualifiers()).foreach {
//      qualifiers =>
//        qualifiers.filter(q => Option(q).isDefined).foreach {
//          qualifier =>
//            bytes = Bytes.add(bytes, qualifier)
//        }
//    }
    //    if (getRequest.family() != null) bytes = Bytes.add(bytes, getRequest.family())
    //    if (getRequest.qualifiers() != null) getRequest.qualifiers().filter(_ != null).foreach(q => bytes = Bytes.add(bytes, q))
//    bytes
  }


  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = {
    val defers: Seq[Deferred[QueryRequestWithResult]] = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
      parentEdges <- prevStepEdges.get(queryRequest.vertex.id)
    } yield fetch(queryRequest, prevStepScore, isInnerCall = false, parentEdges)

    val grouped: Deferred[util.ArrayList[QueryRequestWithResult]] = Deferred.group(defers)
    grouped withCallback {
      queryResults: util.ArrayList[QueryRequestWithResult] =>
        queryResults.toIndexedSeq
    } toFuture
  }
}
