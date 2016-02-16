package com.kakao.s2graph.core.storage.hbase

import java.util
import java.util.Base64
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder}
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.storage._
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.stumbleupon.async.Deferred
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Durability}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.hbase.async._
import scala.collection.JavaConversions._
import scala.collection.{Map, Seq}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, duration}
import scala.util.Random
import scala.util.hashing.MurmurHash3


object AsynchbaseStorage {
  val vertexCf = HSerializable.vertexCf
  val edgeCf = HSerializable.edgeCf
  val emptyKVs = new util.ArrayList[KeyValue]()


  def makeClient(config: Config, overrideKv: (String, String)*) = {
    val asyncConfig: org.hbase.async.Config = new org.hbase.async.Config()

    for (entry <- config.entrySet() if entry.getKey.contains("hbase")) {
      asyncConfig.overrideConfig(entry.getKey, entry.getValue.unwrapped().toString)
    }

    for ((k, v) <- overrideKv) {
      asyncConfig.overrideConfig(k, v)
    }

    val client = new HBaseClient(asyncConfig)
    logger.info(s"Asynchbase: ${client.getConfig.dumpConfiguration()}")
    client
  }
}

class AsynchbaseStorage(override val config: Config)(implicit ec: ExecutionContext)
  extends Storage[Deferred[QueryRequestWithResult]](config) {

  import Extensions.DeferOps

  val configWithFlush = config.withFallback(ConfigFactory.parseMap(Map("hbase.rpcs.buffered_flush_interval" -> "0")))
  val client = AsynchbaseStorage.makeClient(config)

  private val clientWithFlush = AsynchbaseStorage.makeClient(config, "hbase.rpcs.buffered_flush_interval" -> "0")
  private val clients = Seq(client, clientWithFlush)
  private val clientFlushInterval = config.getInt("hbase.rpcs.buffered_flush_interval").toString().toShort
  private val emptyKeyValues = new util.ArrayList[KeyValue]()

  private def client(withWait: Boolean): HBaseClient = if (withWait) clientWithFlush else client

  /** Future Cache to squash request */
  private val futureCache = CacheBuilder.newBuilder()
  .initialCapacity(maxSize)
  .concurrencyLevel(Runtime.getRuntime.availableProcessors())
  .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
  .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Long, (Long, Deferred[QueryRequestWithResult])]()

  /** Simple Vertex Cache */
  private val vertexCache = CacheBuilder.newBuilder()
  .initialCapacity(maxSize)
  .concurrencyLevel(Runtime.getRuntime.availableProcessors())
  .expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
  .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Integer, Option[Vertex]]()


  override def writeToStorage(kv: SKeyValue, withWait: Boolean): Future[Boolean] = {
//        logger.debug(s"$rpc")
    val _client = client(withWait)
    val _defer = kv.operation match {
      case SKeyValue.Put => _client.put(new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp))
      case SKeyValue.Delete =>
        if (kv.qualifier == null) _client.delete(new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp))
        else _client.delete(new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp))
      case SKeyValue.Increment =>
        _client.atomicIncrement(new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)))
    }
    val future = _defer.withCallback { ret => true }.recoverWith { ex =>
      logger.error(s"mutation failed. $kv", ex)
      false
    }.toFuture

    if (withWait) future else Future.successful(true)
  }

  override def flush(): Unit = clients.foreach { client =>
    val timeout = Duration((clientFlushInterval + 10) * 20, duration.MILLISECONDS)
    Await.result(client.flush().toFuture, timeout)
  }


  override def createTable(zkAddr: String,
                           tableName: String,
                           cfs: List[String],
                           regionMultiplier: Int,
                           ttl: Option[Int],
                           compressionAlgorithm: String): Unit = {
    logger.info(s"create table: $tableName on $zkAddr, $cfs, $regionMultiplier, $compressionAlgorithm")
    val admin = getAdmin(zkAddr)
    val regionCount = admin.getClusterStatus.getServersSize * regionMultiplier
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      try {
        val desc = new HTableDescriptor(TableName.valueOf(tableName))
        desc.setDurability(Durability.ASYNC_WAL)
        for (cf <- cfs) {
          val columnDesc = new HColumnDescriptor(cf)
            .setCompressionType(Algorithm.valueOf(compressionAlgorithm.toUpperCase))
            .setBloomFilterType(BloomType.ROW)
            .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
            .setMaxVersions(1)
            .setTimeToLive(2147483647)
            .setMinVersions(0)
            .setBlocksize(32768)
            .setBlockCacheEnabled(true)
          if (ttl.isDefined) columnDesc.setTimeToLive(ttl.get)
          desc.addFamily(columnDesc)
        }

        if (regionCount <= 1) admin.createTable(desc)
        else admin.createTable(desc, getStartKey(regionCount), getEndKey(regionCount), regionCount)
      } catch {
        case e: Throwable =>
          logger.error(s"$zkAddr, $tableName failed with $e", e)
          throw e
      }
    } else {
      logger.info(s"$zkAddr, $tableName, $cfs already exist.")
    }
  }

  /** Mutation Builder */
  override def commitUpdate(edges: Seq[Edge], statusCode: Byte): Future[Boolean] = {
    fetchSnapshotEdge(edges.head).flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>
      val (newEdge, edgeUpdate) = Edge.buildOperation(snapshotEdgeOpt, edges)

      /**
       * there is no need to update so just finish this commit.
       */
      if (edgeUpdate.newSnapshotEdge.isEmpty && statusCode <= 0) {
        logger.debug(s"${newEdge.toLogString} drop.")
        Future.successful(true)
      } else {
        /**
         * process this commit.
         */

        val lockEdge = buildLockEdge(snapshotEdgeOpt, newEdge, kvOpt)
        val releaseLockEdge = buildReleaseLockEdge(snapshotEdgeOpt, lockEdge, edgeUpdate)
        val _process = commitProcess(newEdge, statusCode)(snapshotEdgeOpt, kvOpt)_
        val future = snapshotEdgeOpt match {
          case None =>
            // no one ever did success on acquire lock.
            _process(lockEdge, releaseLockEdge, edgeUpdate)
          //        process(lockEdge, releaseLockEdge, edgeUpdate, statusCode)
          case Some(snapshotEdge) =>
            // someone did success on acquire lock at least one.
            snapshotEdge.pendingEdgeOpt match {
              case None =>
                // not locked
                _process(lockEdge, releaseLockEdge, edgeUpdate)
              //            process(lockEdge, releaseLockEdge, edgeUpdate, statusCode)
              case Some(pendingEdge) =>
                def isLockExpired = pendingEdge.lockTs.get + LockExpireDuration < System.currentTimeMillis()
                if (isLockExpired) {
                  val oldSnapshotEdge = if (snapshotEdge.ts == pendingEdge.ts) None else Option(snapshotEdge)
                  val (_, newEdgeUpdate) = Edge.buildOperation(oldSnapshotEdge, Seq(pendingEdge))
                  val newLockEdge = buildLockEdge(snapshotEdgeOpt, pendingEdge, kvOpt)
                  val newReleaseLockEdge = buildReleaseLockEdge(snapshotEdgeOpt, newLockEdge, newEdgeUpdate)
                  commitProcess(newEdge, statusCode = 0)(snapshotEdgeOpt, kvOpt)(newLockEdge, newReleaseLockEdge, newEdgeUpdate).flatMap { ret =>
                    //              process(newLockEdge, newReleaseLockEdge, newEdgeUpdate, statusCode = 0).flatMap { ret =>
                    val log = s"[Success]: Resolving expired pending edge.\n${pendingEdge.toLogString}"
                    throw new PartialFailureException(newEdge, 0, log)
                  }
                } else {
                  // locked
                  if (pendingEdge.ts == newEdge.ts && statusCode > 0) {
                    // self locked
                    val oldSnapshotEdge = if (snapshotEdge.ts == pendingEdge.ts) None else Option(snapshotEdge)
                    val (_, newEdgeUpdate) = Edge.buildOperation(oldSnapshotEdge, Seq(newEdge))
                    val newReleaseLockEdge = buildReleaseLockEdge(snapshotEdgeOpt, lockEdge, newEdgeUpdate)

                    /** lockEdge will be ignored */
                    _process(lockEdge, newReleaseLockEdge, newEdgeUpdate)
                    //                process(lockEdge, newReleaseLockEdge, newEdgeUpdate, statusCode)
                  } else {
                    throw new PartialFailureException(newEdge, statusCode, s"others[${pendingEdge.ts}] is mutating. me[${newEdge.ts}]")
                  }
                }
            }
        }
        future.map { ret =>
          if (ret) {
            logger.info(s"[Success] commit: \n${edges.map(_.toLogString).mkString("\n")}")
          } else {
            throw new PartialFailureException(newEdge, 3, "commit failed.")
          }
          true
        }
      }
    }
  }


  override def fetchIndexEdgeKeyValues(hbaseRpc: AnyRef): Future[Seq[SKeyValue]] = {
    val defer = fetchKeyValuesInner(hbaseRpc)
    defer.toFuture.map { kvsArr =>
      kvsArr.map { kv =>
        implicitly[CanSKeyValue[KeyValue]].toSKeyValue(kv)
      } toSeq
    }
  }

  override def fetchSnapshotEdgeKeyValues(rpc: AnyRef): Future[Seq[SKeyValue]] = fetchIndexEdgeKeyValues(rpc)


  override def buildRequest(queryRequest: QueryRequest): AnyRef = {
    import HSerializable._
    val queryParam = queryRequest.queryParam
    val label = queryParam.label
    val edge = toRequestEdge(queryRequest)

    val kv = if (queryParam.tgtVertexInnerIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      snapshotEdgeSerializer(snapshotEdge).toKeyValues.head
      //      new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      indexEdgeSerializer(indexedEdge).toKeyValues.head
    }

    val (minTs, maxTs) = queryParam.duration.getOrElse((0L, Long.MaxValue))

    label.schemaVersion match {
      case HBaseType.VERSION4 if queryParam.tgtVertexInnerIdOpt.isEmpty =>
        val scanner = client.newScanner(label.hbaseTableName.getBytes)
        scanner.setFamily(edgeCf)

        /**
         * TODO: remove this part.
         */
        val indexEdgeOpt = edge.edgesWithIndex.filter(edgeWithIndex => edgeWithIndex.labelIndex.seq == queryParam.labelOrderSeq).headOption
        val indexEdge = indexEdgeOpt.getOrElse(throw new RuntimeException(s"Can`t find index for query $queryParam"))

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
//                logger.debug(s"[StartKey]: ${startKey.toList}")
//                logger.debug(s"[StopKey]: ${stopKey.toList}")

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
          if (queryParam.tgtVertexInnerIdOpt.isDefined) new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
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

  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Deferred[QueryRequestWithResult] = {
    def fetchInner(hbaseRpc: AnyRef) = {
      fetchKeyValuesInner(hbaseRpc).withCallback { kvs =>
        val edgeWithScores = toEdges(kvs, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
        val resultEdgesWithScores = if (queryRequest.queryParam.sample >= 0) {
          sample(queryRequest, edgeWithScores, queryRequest.queryParam.sample)
        } else edgeWithScores
        QueryRequestWithResult(queryRequest, QueryResult(resultEdgesWithScores, tailCursor = kvs.lastOption.map(_.key).getOrElse(Array.empty)))

      } recoverWith { ex =>
        logger.error(s"fetchInner failed. fallback return. $hbaseRpc}", ex)
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


  override def fetches(queryRequestWithScoreLs: scala.Seq[(QueryRequest, Double)],
                       prevStepEdges: Predef.Map[VertexId, scala.Seq[EdgeWithScore]]): Future[scala.Seq[QueryRequestWithResult]] = {
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

  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = {
    val defers: Seq[Deferred[(Boolean, Long)]] = for {
      edge <- edges
    } yield {
        val edgeWithIndex = edge.edgesWithIndex.head
        val countWithTs = edge.propsWithTs(LabelMeta.countSeq)
        val countVal = countWithTs.innerVal.toString().toLong
        val incr = buildIncrementsCountAsync(edgeWithIndex, countVal).head
        val request = incr.asInstanceOf[AtomicIncrementRequest]
        client.bufferAtomicIncrement(request) withCallback { resultCount: java.lang.Long =>
          (true, resultCount.longValue())
        } recoverWith { ex =>
          logger.error(s"mutation failed. $request", ex)
          (false, -1L)
        }
      }

    val grouped: Deferred[util.ArrayList[(Boolean, Long)]] = Deferred.groupInOrder(defers)
    grouped.toFuture.map(_.toSeq)
  }



  /** Asynchbase implementation override default getVertices to use future Cache */
  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
    def fromResult(queryParam: QueryParam,
                   kvs: Seq[SKeyValue],
                   version: String): Option[Vertex] = {

      if (kvs.isEmpty) None
      else {
        val newKVs = kvs
        Option(vertexDeserializer.fromKeyValues(queryParam, newKVs, version, None))
      }
    }

    val futures = vertices.map { vertex =>
      val kvs = vertexSerializer(vertex).toKeyValues
      val get = new GetRequest(vertex.hbaseTableName.getBytes, kvs.head.row, HSerializable.vertexCf)
      //      get.setTimeout(this.singleGetTimeout.toShort)
      get.setFailfast(true)
      get.maxVersions(1)

      val cacheKey = MurmurHash3.stringHash(get.toString)
      val cacheVal = vertexCache.getIfPresent(cacheKey)
      if (cacheVal == null)
        fetchIndexEdgeKeyValues(get).map { kvs =>
          fromResult(QueryParam.Empty, kvs, vertex.serviceColumn.schemaVersion)
        } recoverWith { case ex: Throwable =>
          Future.successful(None)
        }

      else Future.successful(cacheVal)
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }

  /**
   * Private Methods which is specific to Asynchbase implementation.
   */


  private  def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}").mkString("\n")
    logger.debug(msg)
  }

  private def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge, edgeMutate: EdgeMutate) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}",
      s"${edgeMutate.toLogString}").mkString("\n")
    logger.debug(msg)
  }

  private def buildLockEdge(snapshotEdgeOpt: Option[Edge], edge: Edge, kvOpt: Option[SKeyValue]) = {
    val currentTs = System.currentTimeMillis()
    val lockTs = snapshotEdgeOpt match {
      case None => Option(currentTs)
      case Some(snapshotEdge) =>
        snapshotEdge.pendingEdgeOpt match {
          case None => Option(currentTs)
          case Some(pendingEdge) => pendingEdge.lockTs
        }
    }
    val newVersion = kvOpt.map(_.timestamp).getOrElse(edge.ts) + 1
    //      snapshotEdgeOpt.map(_.version).getOrElse(edge.ts) + 1
    val pendingEdge = edge.copy(version = newVersion, statusCode = 1, lockTs = lockTs)
    val base = snapshotEdgeOpt match {
      case None =>
        // no one ever mutated on this snapshotEdge.
        edge.toSnapshotEdge.copy(pendingEdgeOpt = Option(pendingEdge))
      case Some(snapshotEdge) =>
        // there is at least one mutation have been succeed.
        snapshotEdgeOpt.get.toSnapshotEdge.copy(pendingEdgeOpt = Option(pendingEdge))
    }
    base.copy(version = newVersion, statusCode = 1, lockTs = None)
  }

  private def buildReleaseLockEdge(snapshotEdgeOpt: Option[Edge], lockEdge: SnapshotEdge,
                                     edgeMutate: EdgeMutate) = {
    val newVersion = lockEdge.version + 1
    val base = edgeMutate.newSnapshotEdge match {
      case None =>
        // shouldReplace false
        assert(snapshotEdgeOpt.isDefined)
        snapshotEdgeOpt.get.toSnapshotEdge
      case Some(newSnapshotEdge) => newSnapshotEdge
    }
    base.copy(version = newVersion, statusCode = 0, pendingEdgeOpt = None)
  }

  private def acquireLock(statusCode: Byte,
                            edge: Edge,
                            oldSnapshotEdgeOpt: Option[Edge],
                            lockEdge: SnapshotEdge,
                            oldBytes: Array[Byte]): Future[Boolean] = {
    if (statusCode >= 1) {
      logger.debug(s"skip acquireLock: [$statusCode]\n${edge.toLogString}")
      Future.successful(true)
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) throw new PartialFailureException(edge, 0, s"$p")
      else {
        val lockEdgePut = snapshotEdgeSerializer(lockEdge).toKeyValues.head
        val oldPut = oldSnapshotEdgeOpt.map(e => snapshotEdgeSerializer(e.toSnapshotEdge).toKeyValues.head)
        //        val lockEdgePut = buildPutAsync(lockEdge).head
        //        val oldPut = oldSnapshotEdgeOpt.map(e => buildPutAsync(e.toSnapshotEdge).head)
        writeLock(lockEdgePut, oldPut).recoverWith { case ex: Exception =>
          logger.error(s"AcquireLock RPC Failed.")
          throw new PartialFailureException(edge, 0, "AcquireLock RPC Failed")
        }.map { ret =>
          if (ret) {
            val log = Seq(
              "\n",
              "=" * 50,
              s"[Success]: acquireLock",
              s"[RequestEdge]: ${edge.toLogString}",
              s"[LockEdge]: ${lockEdge.toLogString()}",
              s"[PendingEdge]: ${lockEdge.pendingEdgeOpt.map(_.toLogString).getOrElse("")}",
              "=" * 50, "\n").mkString("\n")

            logger.debug(log)
            //            debug(ret, "acquireLock", edge.toSnapshotEdge)
          } else {
            throw new PartialFailureException(edge, 0, "hbase fail.")
          }
          true
        }
      }
    }
  }



  private def releaseLock(predicate: Boolean,
                            edge: Edge,
                            lockEdge: SnapshotEdge,
                            releaseLockEdge: SnapshotEdge,
                            _edgeMutate: EdgeMutate,
                            oldBytes: Array[Byte]): Future[Boolean] = {
    if (!predicate) {
      throw new PartialFailureException(edge, 3, "predicate failed.")
    }
    val p = Random.nextDouble()
    if (p < FailProb) throw new PartialFailureException(edge, 3, s"$p")
    else {
      val releaseLockEdgePut = snapshotEdgeSerializer(releaseLockEdge).toKeyValues.head
      val lockEdgePut = snapshotEdgeSerializer(lockEdge).toKeyValues.head
      writeLock(releaseLockEdgePut, Option(lockEdgePut)).recoverWith {
        case ex: Exception =>
          logger.error(s"ReleaseLock RPC Failed.")
          throw new PartialFailureException(edge, 3, "ReleaseLock RPC Failed")
      }.map { ret =>
        if (ret) {
          debug(ret, "releaseLock", edge.toSnapshotEdge)
        } else {
          val msg = Seq("\nFATAL ERROR\n",
            "=" * 50,
            oldBytes.toList,
            lockEdgePut,
            releaseLockEdgePut,
            //            lockEdgePut.value.toList,
            //            releaseLockEdgePut.value().toList,
            "=" * 50,
            "\n"
          )
          logger.error(msg.mkString("\n"))
          //          error(ret, "releaseLock", edge.toSnapshotEdge)
          throw new PartialFailureException(edge, 3, "hbase fail.")
        }
        true
      }
    }
    Future.successful(true)
  }


  private def mutate(predicate: Boolean,
                       edge: Edge,
                       statusCode: Byte,
                       _edgeMutate: EdgeMutate): Future[Boolean] = {
    if (!predicate) throw new PartialFailureException(edge, 1, "predicate failed.")

    if (statusCode >= 2) {
      logger.debug(s"skip mutate: [$statusCode]\n${edge.toLogString}")
      Future.successful(true)
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) throw new PartialFailureException(edge, 1, s"$p")
      else
        writeAsyncSimple(edge.label.hbaseZkAddr, indexedEdgeMutations(_edgeMutate), withWait = true).map { ret =>
          if (ret) {
            debug(ret, "mutate", edge.toSnapshotEdge, _edgeMutate)
          } else {
            throw new PartialFailureException(edge, 1, "hbase fail.")
          }
          true
        }
    }
  }

  private def increment(predicate: Boolean,
                          edge: Edge,
                          statusCode: Byte, _edgeMutate: EdgeMutate): Future[Boolean] = {
    if (!predicate) throw new PartialFailureException(edge, 2, "predicate failed.")
    if (statusCode >= 3) {
      logger.debug(s"skip increment: [$statusCode]\n${edge.toLogString}")
      Future.successful(true)
    } else {
      val p = Random.nextDouble()
      if (p < FailProb) throw new PartialFailureException(edge, 2, s"$p")
      else
        writeAsyncSimple(edge.label.hbaseZkAddr, increments(_edgeMutate), withWait = true).map { ret =>
          if (ret) {
            debug(ret, "increment", edge.toSnapshotEdge, _edgeMutate)
          } else {
            throw new PartialFailureException(edge, 2, "hbase fail.")
          }
          true
        }
    }
  }


  /** this may be overrided by specific storage implementation */
  private def commitProcess(edge: Edge, statusCode: Byte)
                             (snapshotEdgeOpt: Option[Edge], kvOpt: Option[SKeyValue])
                             (lockEdge: SnapshotEdge, releaseLockEdge: SnapshotEdge, _edgeMutate: EdgeMutate): Future[Boolean] = {
    val oldBytes = kvOpt.map(kv => kv.value).getOrElse(Array.empty[Byte])
    for {
      locked <- acquireLock(statusCode, edge, snapshotEdgeOpt, lockEdge, oldBytes)
      mutated <- mutate(locked, edge, statusCode, _edgeMutate)
      incremented <- increment(mutated, edge, statusCode, _edgeMutate)
      released <- releaseLock(incremented, edge, lockEdge, releaseLockEdge, _edgeMutate, oldBytes)
    } yield {
      released
    }
  }



  private def fetchKeyValuesInner(rpc: AnyRef): Deferred[util.ArrayList[KeyValue]] = {
    rpc match {
      case getRequest: GetRequest => client.get(getRequest)
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
          logger.error(s"fetchKeyValuesInner failed.", ex)
          scanner.close()
          emptyKeyValues
        }
      case _ => Deferred.fromError(new RuntimeException(s"fetchKeyValues failed. $rpc"))
    }
  }

  private def writeLock(rpc: SKeyValue, expectedOpt: Option[SKeyValue]): Future[Boolean] = {
    val put = new PutRequest(rpc.table, rpc.row, rpc.cf, rpc.qualifier, rpc.value, rpc.timestamp)
    val expected = expectedOpt.map(_.value).getOrElse(Array.empty)
    client(withWait = true).compareAndSet(put, expected).withCallback(ret => ret.booleanValue()).toFuture
  }


  private def toCacheKeyBytes(hbaseRpc: AnyRef): Array[Byte] = {
    hbaseRpc match {
      case getRequest: GetRequest => getRequest.key()
      case scanner: Scanner => scanner.getCurrentKey()
      case _ =>
        logger.error(s"toCacheKeyBytes failed. not supported class type. $hbaseRpc")
        Array.empty[Byte]
    }
  }

  private def getAdmin(zkAddr: String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkAddr)
    val conn = ConnectionFactory.createConnection(conf)
    conn.getAdmin
  }
  private def enableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).enableTable(TableName.valueOf(tableName))
  }

  private def disableTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
  }

  private def dropTable(zkAddr: String, tableName: String) = {
    getAdmin(zkAddr).disableTable(TableName.valueOf(tableName))
    getAdmin(zkAddr).deleteTable(TableName.valueOf(tableName))
  }

  private def getStartKey(regionCount: Int): Array[Byte] = {
    Bytes.toBytes((Int.MaxValue / regionCount))
  }

  private def getEndKey(regionCount: Int): Array[Byte] = {
    Bytes.toBytes((Int.MaxValue / regionCount * (regionCount - 1)))
  }


}