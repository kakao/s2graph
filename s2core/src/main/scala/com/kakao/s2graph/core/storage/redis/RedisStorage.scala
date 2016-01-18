package com.kakao.s2graph.core.storage.redis

import com.google.common.cache.Cache
import com.kakao.s2graph.core.GraphExceptions.{FetchTimeoutException, PartialFailureException}
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core.storage._
import com.kakao.s2graph.core.utils.{AsyncRedisClient, logger}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

/**
 * Redis storage handler class
 *
 * Created by june.kay on 2015. 12. 31..
 *
 */
class RedisStorage(val config: Config, vertexCache: Cache[Integer, Option[Vertex]])
                  (implicit ec: ExecutionContext) extends Storage {


  implicit val akkaSystem = akka.actor.ActorSystem()
  lazy val client = new AsyncRedisClient(config)

  // initialize just once ( so we use `val` not `def` ) -> validate once
  val cacheOpt = None

  val MaxRetryNum = config.getInt("max.retry.number")
  val MaxBackOff = config.getInt("max.back.off")
  val DeleteAllFetchSize = config.getInt("delete.all.fetch.size")
  val FailProb = config.getDouble("hbase.fail.prob")
  val LockExpireDuration = Math.max(MaxRetryNum * MaxBackOff * 2, 10000)
  val RedisZsetScore = 1

  val snapshotEdgeDeserializer = new RedisSnapshotEdgeDeserializable
  val indexEdgeDeserializer = new RedisIndexEdgeDeserializable
  val vertexDeserializer = new RedisVertexDeserializable

  val queryBuilder = new RedisQueryBuilder(this)(ec)
  val mutationBuilder = new RedisMutationBuilder(this)(ec)

  // Serializer/Deserializer
  def indexEdgeSerializer(indexedEdge: IndexEdge): StorageSerializable[IndexEdge] =
    new RedisIndexEdgeSerializable(indexedEdge)

  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): StorageSerializable[SnapshotEdge] =
    new RedisSnapshotEdgeSerializable(snapshotEdge)

  def vertexSerializer(vertex: Vertex): StorageSerializable[Vertex] =
    new RedisVertexSerializable(vertex)

  override def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = {
    logger.info(s">> Check edges for Redis ::")
    val futures = for {
      (srcVertex, tgtVertex, queryParam) <- params
    } yield queryBuilder.getEdge(srcVertex, tgtVertex, queryParam, false)

    Future.sequence(futures)
  }

  override def flush(): Unit = ???

  override def vertexCacheOpt: Option[Cache[Integer, Option[Vertex]]] = ???

  def toHex(b: Array[Byte]): String = {
    val tmp = b.map("%02x".format(_)).mkString("\\x")
    if ( tmp.isEmpty ) "" else "\\x" + tmp
  }

  def get(get: RedisGetRequest): Future[Set[SKeyValue]] = {
    logger.info(s">> RedisGet get")
    Future[Set[SKeyValue]] {
      // send rpc call to Redis instance
      client.doBlockWithKey[Set[SKeyValue]]("" /* sharding key */) { jedis =>
        logger.info(s">> jedis gogo; key : ${toHex(get.key)}, min : ${toHex(get.min)}, max : ${toHex(get.max)}, offset :${get.offset}, count : ${get.count}")
        val result = jedis.zrangeByLex(get.key, get.min, get.max, get.offset, get.count).toSet[Array[Byte]].map(v =>
          SKeyValue(Array.empty[Byte], get.key, Array.empty[Byte], Array.empty[Byte], v, 0L)
        )
        if (get.isIncludeDegree) {
          val degreeBytes = jedis.get(get.degreeEdgeKey)
          result + SKeyValue(Array.empty[Byte], get.key, Array.empty[Byte], Array.empty[Byte], degreeBytes, 0L)
        } else result
      } match {
        case Success(v) =>
          logger.error(s">> get success!! $v")
          v
        case Failure(e) =>
          logger.error(s">> get fail!! $e")
          Set[SKeyValue]()
      }
    }
  }

  private def writeToStorage(rpc: RedisRPC): Future[Boolean] = {
    Future[Boolean] {
      client.doBlockWithKey[Boolean]("" /* sharding key */) { jedis =>
        println(s">> [writeToStorage] ")
        val write = rpc match {
          case d: RedisDeleteRequest => if (jedis.zrem(d.key, d.value) == 1) true else false
          case p: RedisPutRequest if p.qualifier.length > 0 => // Edge put operation
            println(s">> [writeToStorage] edge put : $p")
            if (jedis.zadd(p.key, RedisZsetScore, p.value) == 1) true else false
          case p: RedisPutRequest if p.qualifier.length == 0 =>  // Vertex put operation
            println(s">> [writeToStorage] vertex put : $p")
            if (jedis.zadd(p.key, RedisZsetScore, p.qualifier ++ p.value) == 1) true else false
          case i: RedisAtomicIncrementRequest =>
            println(s">> [writeToStorage] Atomic increment : $i")
            atomicIncrement(i)
        }
        write
      } match {
        case Success(v) => v
        case Failure(e) => false
      }
    }
  }

  def compareAndSet(putRequest: RedisPutRequest, oldBytes: Array[Byte]): Future[Boolean] = {
    Future[Boolean] {
      client.doBlockWithKey[Boolean]("" /* shard key */) { jedis =>
        jedis.watch(putRequest.key)


        val script: String =
          """local key = KEYS[1]
            |  local minMax = ARGV[1]
            |  local oldData = ARGV[2]
            |  local value = ARGV[3]
            |  local score = ARGV[4]
            |  local data = redis.call('ZRANGEBYLEX', key, minMax, minMax)[1]
            |  if data == oldData then
            |    redis.call('ZREM', key, data)
            |    return redis.call('ZADD', key, score, value)
            |  elseif data == nil then
            |    return redis.call('ZADD', key, score, value)
            |  end
            |  return 0
          """.stripMargin


        logger.error(s">> start compareAndSet")
        val t = jedis.multi()
//        val inclusivePaddedBytes = Bytes.add(Array.fill[Byte](1)(Bytes.toBytes("[").head), putRequest.value)
//        logger.error(s">> inclusivePadded : $inclusivePaddedBytes")
//        val resp = t.zrangeByLex(putRequest.key, inclusivePaddedBytes, inclusivePaddedBytes, 0, 1)
//        logger.error(s">> resp : $resp")
//        resp
//        val newBytes = resp.get().headOption.getOrElse(Array.empty[Byte])
//        logger.error(s">> newBytes : $newBytes")
//        val rtn = if (oldBytes.equals(newBytes)) {
//          logger.error(s">> compare result - equals ")
//          if (t.zrem(putRequest.key, oldBytes).get() == 1) {
//            t.zadd(putRequest.key, RedisZsetScore, putRequest.value)
//            true
//          } else false
//        } else false
        if ( oldBytes.length == 0 ) {
          t.zadd(putRequest.key, RedisZsetScore, putRequest.value)
        } else {
          val keys = List[String](GraphUtil.bytesToHexString(putRequest.key))
          val minMax = "[" + GraphUtil.bytesToHexString(oldBytes)
          val argv = List[String](minMax, GraphUtil.bytesToHexString(oldBytes), GraphUtil.bytesToHexString(putRequest.value), GraphUtil.bytesToHexString(Bytes.toBytes(RedisZsetScore)))

          t.eval(script, keys, argv)
        }

//        t.zrem(putRequest.key, oldBytes)
//        t.zadd(putRequest.key, RedisZsetScore, putRequest.value)

        t.exec()


        jedis.unwatch()

        true
      } match {
        case Success(v) =>
          logger.error(s">> success compareAndSet : $v")
          v
        case Failure(e) =>
          e.printStackTrace()
          logger.error(s">> failure compareAndSet : $e")
          false
      }
    }
  }

  def atomicIncrement(req: RedisAtomicIncrementRequest): Boolean = {
    client.doBlockWithKey[Boolean]("" /* shard key */) { jedis =>
      println(s">> [atomicIncrement] key : ${GraphUtil.bytesToHexString(req.key)}, value : ${GraphUtil.bytesToHexString(req.value)}, delta : ${req.delta}")
      jedis.watch(req.key)

      // TODO Do we need to add transaction - multi?
      jedis.incrBy(req.degreeEdgeKey, req.delta)

      jedis.unwatch()
      true
    } match {
      case Success(v) =>
        logger.error(s">> get success!! $v")
        true
      case Failure(e) =>
        logger.error(s">> get fail!! $e")
        false

    }
  }


  // Interface
  override def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = ???

  override def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] = ???

  override def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = ???

  override def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = ???

  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = {
    //    mutateEdgeWithOp(edge, withWait)
    val strongConsistency = edge.label.consistencyLevel == "strong"
    val edgeFuture =
      if (edge.op == GraphUtil.operations("delete") && !strongConsistency) {
        val (_, edgeUpdate) = Edge.buildDeleteBulk(None, edge)
        val mutations =
          mutationBuilder.indexedEdgeMutations(edgeUpdate) ++
            mutationBuilder.snapshotEdgeMutations(edgeUpdate) ++
            mutationBuilder.increments(edgeUpdate)
        writeAsyncSimple(mutations, withWait)
      } else {
        mutateEdgesInner(Seq(edge), strongConsistency, withWait)(Edge.buildOperation)
      }

    val vertexFuture = writeAsyncSimple(mutationBuilder.buildVertexPutsAsync(edge), withWait)
    Future.sequence(Seq(edgeFuture, vertexFuture)).map { rets => rets.forall(identity) }
  }

  private def writeAsyncSimple(rpcs: Seq[RedisRPC], withWait: Boolean): Future[Boolean] = {
    if (rpcs.isEmpty)
      Future.successful(true)
    else {
      // TODO: Handle RPCs in bulks instead of one by one.
      val writes = rpcs.map { rpc => writeToStorage(rpc) }
      if (withWait)
        Future.sequence(writes) map { arr => arr.forall(identity) }
      else
        Future.successful(true)
    }


  }

  private def fetchSnapshotEdge(edge: Edge): Future[(QueryParam, Option[Edge], Option[SKeyValue])] = {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(edge.tgtVertex.innerId))
    val q = Query.toQuery(Seq(edge.srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, edge.srcVertex, _queryParam)

    println(s">> [fetchSanps")


    get(queryBuilder.buildRequest(queryRequest)) map { s =>
      logger.error(s">> $s")
      val edgeOpt = toEdges(s.toSeq, queryParam, 1.0, isInnerCall = true, parentEdges = Nil).headOption.map(_.edge)
      (queryParam, edgeOpt, s.headOption)
    }
  }

  def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge) = {
    val msg = Seq(s"[$ret] [$phase]", s"${snapshotEdge.toLogString()}").mkString("\n")
    logger.debug(msg)
  }

  def debug(ret: Boolean, phase: String, snapshotEdge: SnapshotEdge, edgeMutate: EdgeMutate) = {
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
    // snapshotEdgeOpt.map(_.version).getOrElse(edge.ts) + 1
    val pendingEdge = edge.copy(version = newVersion, statusCode = 1, lockTs = lockTs)
    val base = snapshotEdgeOpt match {
      case None =>
        // no mutation has been conducted on this snapshotEdge.
        edge.toSnapshotEdge.copy(pendingEdgeOpt = Option(pendingEdge))
      case Some(snapshotEdge) =>
        // at least one mutation has been done.
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

  def mutate(predicate: Boolean,
             edge: Edge,
             statusCode: Byte,
             _edgeMutate: EdgeMutate): Future[Boolean] = {
    if (!predicate) throw new PartialFailureException(edge, 1, "predicate failed.")

    if (statusCode >= 2) {
      logger.debug(s"skip mutate: [$statusCode]\n${edge.toLogString}")
      Future.successful(true)
    } else {
      logger.error(s">> mutate start")
      val p = Random.nextDouble()
      if (p < FailProb) throw new PartialFailureException(edge, 1, s"$p")
      else
        writeAsyncSimple(mutationBuilder.indexedEdgeMutations(_edgeMutate), withWait = true).map { ret =>
          if (ret) {
            debug(ret, "mutate", edge.toSnapshotEdge, _edgeMutate)
          } else {
            throw new PartialFailureException(edge, 1, "hbase fail.")
          }
          true
        }
    }
  }

  def increment(predicate: Boolean,
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
        writeAsyncSimple(mutationBuilder.increments(_edgeMutate), withWait = true).map { ret =>
          if (ret) {
            debug(ret, "increment", edge.toSnapshotEdge, _edgeMutate)
          } else {
            throw new PartialFailureException(edge, 2, "hbase fail.")
          }
          true
        }
    }
  }

  def acquireLock(statusCode: Byte, edge: Edge,
                  lockEdge: SnapshotEdge, oldBytes: Array[Byte]): Future[Boolean] =
    if (statusCode >= 1) {
      logger.debug(s"skip acquireLock: [$statusCode]\n${edge.toLogString}")
      Future.successful(true)
    } else {
      logger.error(s">> acquireLock start")
      val p = Random.nextDouble()
      if (p < FailProb) throw new PartialFailureException(edge, 0, s"$p")
      else {
        val lockEdgePut = toPutRequest(lockEdge)
        logger.error(s">> start compareAndSet")
        compareAndSet(lockEdgePut, oldBytes).recoverWith {
          case ex: Exception =>
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
          } else {
            throw new PartialFailureException(edge, 0, "hbase fail.")
          }
          true
        }
      }
    }

  def releaseLock(predicate: Boolean,
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
      val releaseLockEdgePut = toPutRequest(releaseLockEdge)
      val lockEdgePut = toPutRequest(lockEdge)

      println(s">> [releaseLock] start compareAndSet")
      compareAndSet(releaseLockEdgePut, lockEdgePut.value).recoverWith {
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
            lockEdgePut.value.toList,
            releaseLockEdgePut.value.toList,
            "=" * 50,
            "\n"
          )
          logger.error(msg.mkString("\n"))
          throw new PartialFailureException(edge, 3, "hbase fail.")
        }
        true
      }
    }
  }

  private def toPutRequest(snapshotEdge: SnapshotEdge): RedisPutRequest = {
    mutationBuilder.buildPutAsync(snapshotEdge).head.asInstanceOf[RedisPutRequest]
  }

  private def commitUpdate(edge: Edge,
    statusCode: Byte)(snapshotEdgeOpt: Option[Edge],
      kvOpt: Option[SKeyValue],
      edgeUpdate: EdgeMutate): Future[Boolean] = {
    def oldBytes = kvOpt.map(_.value).getOrElse(Array.empty)

    def process(lockEdge: SnapshotEdge,
                releaseLockEdge: SnapshotEdge,
                _edgeMutate: EdgeMutate,
                statusCode: Byte): Future[Boolean] = {

      logger.error(s">> state machine start")
      for {
        locked <- acquireLock(statusCode, edge, lockEdge, oldBytes)
        mutated <- mutate(locked, edge, statusCode, _edgeMutate)
        incremented <- increment(mutated, edge, statusCode, _edgeMutate)
        released <- releaseLock(incremented, edge, lockEdge, releaseLockEdge, _edgeMutate, oldBytes)
      } yield {
        released
      }
    }


    val lockEdge = buildLockEdge(snapshotEdgeOpt, edge, kvOpt)
    val releaseLockEdge = buildReleaseLockEdge(snapshotEdgeOpt, lockEdge, edgeUpdate)
    logger.error(s">> lockEdge: $lockEdge, releaseEdge :$releaseLockEdge")

    snapshotEdgeOpt match {
      case None =>
        // 'acquire lock' has never been conducted.
        process(lockEdge, releaseLockEdge, edgeUpdate, statusCode)
      case Some(snapshotEdge) =>
        // at least one 'acquire lock' has been conducted.
        snapshotEdge.pendingEdgeOpt match {
          case None =>
            // not locked
            process(lockEdge, releaseLockEdge, edgeUpdate, statusCode)
          case Some(pendingEdge) =>
            def isLockExpired = pendingEdge.lockTs.get + LockExpireDuration < System.currentTimeMillis()
            if (isLockExpired) {
              // if snapshot and pending edge's ts are same, it is first edge operation for current src/tgt edge
              val oldSnapshotEdge = if (snapshotEdge.ts == pendingEdge.ts) None else Option(snapshotEdge)
              val (_, newEdgeUpdate) = Edge.buildOperation(oldSnapshotEdge, Seq(pendingEdge))
              val newLockEdge = buildLockEdge(snapshotEdgeOpt, pendingEdge, kvOpt)
              val newReleaseLockEdge = buildReleaseLockEdge(snapshotEdgeOpt, newLockEdge, newEdgeUpdate)
              process(newLockEdge, newReleaseLockEdge, newEdgeUpdate, statusCode = 0).flatMap { ret =>
                val log = s"[Success]: Resolving expired pending edge.\n${pendingEdge.toLogString}"
                throw new PartialFailureException(edge, 0, log)
              }
            } else {
              // locked
              if (pendingEdge.ts == edge.ts && statusCode > 0) {
                // self locked
                val oldSnapshotEdge = if (snapshotEdge.ts == pendingEdge.ts) None else Option(snapshotEdge)
                val (_, newEdgeUpdate) = Edge.buildOperation(oldSnapshotEdge, Seq(edge))
                val newReleaseLockEdge = buildReleaseLockEdge(snapshotEdgeOpt, lockEdge, newEdgeUpdate)

                /** lockEdge will be ignored */
                process(lockEdge, newReleaseLockEdge, newEdgeUpdate, statusCode)
              } else {
                throw new PartialFailureException(edge, statusCode, s"others[${pendingEdge.ts}] is mutating. me[${edge.ts}]")
              }
            }
        }
    }
  }


  private def mutateEdgesInner(edges: Seq[Edge],
                               checkConsistency: Boolean,
                               withWait: Boolean)(f: (Option[Edge], Seq[Edge]) => (Edge, EdgeMutate)): Future[Boolean] = {
    if (!checkConsistency) {
      val futures = edges.map { edge =>
        val (_, edgeUpdate) = f(None, Seq(edge))
        val mutations =
          mutationBuilder.indexedEdgeMutations(edgeUpdate) ++
            mutationBuilder.snapshotEdgeMutations(edgeUpdate) ++
            mutationBuilder.increments(edgeUpdate)
        writeAsyncSimple(mutations, withWait)
      }
      Future.sequence(futures).map { rets => rets.forall(identity) }
    } else {
      def commit(_edges: Seq[Edge], statusCode: Byte): Future[Boolean] = {

        fetchSnapshotEdge(_edges.head) flatMap { case (queryParam, snapshotEdgeOpt, kvOpt) =>

          logger.error(s">> snapshot edge fetched  : $snapshotEdgeOpt, $kvOpt")
          val (newEdge, edgeUpdate) = f(snapshotEdgeOpt, _edges)
          logger.error(s">> buildOperation completed  : $newEdge, $edgeUpdate")
          //shouldReplace false.
          if (edgeUpdate.newSnapshotEdge.isEmpty && statusCode <= 0) {
            logger.debug(s"${newEdge.toLogString} drop.")
            Future.successful(true)
          } else {
            logger.error(s">> start commit update")
            commitUpdate(newEdge, statusCode)(snapshotEdgeOpt, kvOpt, edgeUpdate).map { ret =>
              if (ret) {
                logger.info(s"[Success] commit: \n${_edges.map(_.toLogString).mkString("\n")}")
              } else {
                throw new PartialFailureException(newEdge, 3, "commit failed.")
              }
              true
            }
          }
        }
      }
      def retry(tryNum: Int)(edges: Seq[Edge], statusCode: Byte)(fn: (Seq[Edge], Byte) => Future[Boolean]): Future[Boolean] = {
        if (tryNum >= MaxRetryNum) {
          edges.foreach { edge =>
            logger.error(s"commit failed after $MaxRetryNum\n${edge.toLogString}")
            ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge))
          }
          Future.successful(false)
        } else {
          val future = fn(edges, statusCode)
          future.onSuccess {
            case success =>
              logger.debug(s"Finished. [$tryNum]\n${edges.head.toLogString}\n")
          }
          future recoverWith {
            case FetchTimeoutException(retryEdge) =>
              logger.info(s"[Try: $tryNum], Fetch fail.\n${retryEdge}")
              retry(tryNum + 1)(edges, statusCode)(fn)

            case PartialFailureException(retryEdge, failedStatusCode, faileReason) =>
              val status = failedStatusCode match {
                case 0 => "AcquireLock failed."
                case 1 => "Mutation failed."
                case 2 => "Increment failed."
                case 3 => "ReleaseLock failed."
                case 4 => "Unknown"
              }

              Thread.sleep(Random.nextInt(MaxBackOff))
              logger.info(s"[Try: $tryNum], [Status: $status] partial fail.\n${retryEdge.toLogString}\nFailReason: ${faileReason}")
              retry(tryNum + 1)(Seq(retryEdge), failedStatusCode)(fn)
            case ex: Exception =>
              logger.error("Unknown exception", ex)
              Future.successful(false)
          }
        }
      }
      retry(1)(edges, 0)(commit)
    }
  }


  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = ???
}
