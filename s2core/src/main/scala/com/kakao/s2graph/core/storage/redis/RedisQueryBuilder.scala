package com.kakao.s2graph.core.storage.redis

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.QueryBuilder
import com.kakao.s2graph.core.utils.logger
import org.apache.hadoop.hbase.util.Bytes

import scala.annotation.tailrec
import scala.collection.{Seq, Map}
import scala.concurrent.{Future, ExecutionContext, Promise}
import scala.util.Random

/**
  * Created by june.kay with jojo on 2015. 12. 31..
  */
class RedisQueryBuilder(storage: RedisStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[RedisGetRequest, Future[QueryRequestWithResult]](storage) {

  val maxSize = storage.config.getInt("future.cache.max.size")
  val expireAfterWrite = storage.config.getInt("future.cache.expire.after.write")
  val expireAfterAccess = storage.config.getInt("future.cache.expire.after.access")

  val futureCache = CacheBuilder.newBuilder()
    .initialCapacity(maxSize)
    .concurrencyLevel(Runtime.getRuntime.availableProcessors())
    .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
    .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
    .maximumSize(maxSize).build[java.lang.Long, (Long, Future[QueryRequestWithResult])]()


  def buildRequest(queryRequest: QueryRequest): RedisGetRequest = {
    val srcVertex = queryRequest.vertex

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
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      storage.indexEdgeSerializer(indexedEdge).toKeyValues.head
    }

    // Redis supports client-side sharding and does not require hash key so remove heading hash key(2 bytes)
    val rowkey = kv.row.takeRight(kv.row.length - 2)

    // 1. RedisGet instance initialize
    val get = new RedisGetRequest(rowkey)

    // 2. set filter and min/max value's key build
    val (minTs, maxTs) = queryParam.duration.getOrElse(-1L -> -1L)
    val (min, max) = (queryParam.columnRangeFilterMinBytes, queryParam.columnRangeFilterMaxBytes)
    get.setCount(queryParam.limit)
      .setOffset(queryParam.offset)
      .setTimeout(queryParam.rpcTimeoutInMillis)
      .setFilter(min, true, max, true, minTs, maxTs)
  }

  def getEdge(srcVertex: Vertex,
    tgtVertex: Vertex,
    queryParam: QueryParam,
    isInnerCall: Boolean): Future[QueryRequestWithResult] = {
    logger.info(s">> RedisQuerybuilder getEdge::")

    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(tgtVertex.innerId))
    val q = Query.toQuery(Seq(srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, srcVertex, _queryParam)
    fetch(queryRequest, 1.0, isInnerCall = true, parentEdges = Nil)
  }

  def fetch(queryRequest: QueryRequest,
    prevStepScore: Double,
    isInnerCall: Boolean,
    parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = {

    @tailrec
    def randomInt(sampleNumber: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
      if (set.size == sampleNumber) set
      else randomInt(sampleNumber, range, set + Random.nextInt(range))
    }

    def sample(edges: Seq[EdgeWithScore], n: Int): Seq[EdgeWithScore] = {
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

    def fetchInner(request: RedisGetRequest) = {
      storage.get(request) map { values =>
        val edgeWithScores = storage.toEdges(values.toSeq, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
        val resultEdgesWithScores = if (queryRequest.queryParam.sample >= 0 ) {
          sample(edgeWithScores, queryRequest.queryParam.sample)
        } else edgeWithScores
        QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores))
      } recover {
        case e: Exception =>
          logger.error(s"fetchQueryParam failed. fallback return.", e)
          QueryRequestWithResult(queryRequest, QueryResult(isFailure = true))
      }
    }

    def checkAndExpire(request: RedisGetRequest,
                       cacheKey: Long,
                       cacheTTL: Long,
                       cachedAt: Long,
                       defer: Future[QueryRequestWithResult]): Future[QueryRequestWithResult] = {
      if (System.currentTimeMillis() >= cachedAt + cacheTTL) {
        // future is too old. so need to expire and fetch new data from storage.
        futureCache.asMap().remove(cacheKey)
        val newPromise = Promise[QueryRequestWithResult]()
        val newFuture = newPromise.future
        futureCache.asMap().putIfAbsent(cacheKey, (System.currentTimeMillis(), newFuture)) match {
          case null =>
            // only one thread succeed to come here concurrently
            // initiate fetch to storage then add callback on complete to finish promise.
            fetchInner(request) map { queryRequestWithResult =>
              newPromise.trySuccess(queryRequestWithResult)
              queryRequestWithResult
            }
            newFuture
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
          val promise = Promise[QueryRequestWithResult]()
          val future = promise.future
          val now = System.currentTimeMillis()
          val (cachedAt, defer) = futureCache.asMap().putIfAbsent(cacheKey, (now, future)) match {
            case null =>
              fetchInner(request) map { queryRequestWithResult =>
                promise.trySuccess(queryRequestWithResult)
                queryRequestWithResult
              }
              (now, future)
            case oldVal => oldVal
          }
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
        case (cachedAt, defer) =>
          checkAndExpire(request, cacheKey, cacheTTL, cachedAt, defer)
      }
    }


  }

  def toCacheKeyBytes(request: RedisGetRequest): Array[Byte] = ???

  def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = ???
}
