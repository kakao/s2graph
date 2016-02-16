package com.kakao.s2graph.core.storage

import java.util.Base64


import com.kakao.s2graph.core.ExceptionHandler.{Key, Val, KafkaMessage}
import com.kakao.s2graph.core.GraphExceptions.FetchTimeoutException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.storage.hbase._
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.ProducerRecord

import scala.annotation.tailrec
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

abstract class Storage[R](val config: Config)(implicit ec: ExecutionContext) {

  /** storage dependent configurations */
  val MaxRetryNum = config.getInt("max.retry.number")
  val MaxBackOff = config.getInt("max.back.off")
  val DeleteAllFetchSize = config.getInt("delete.all.fetch.size")
  val FailProb = config.getDouble("hbase.fail.prob")
  val LockExpireDuration = Math.max(MaxRetryNum * MaxBackOff * 2, 10000)
  val maxSize = config.getInt("future.cache.max.size")
  val expireAfterWrite = config.getInt("future.cache.expire.after.write")
  val expireAfterAccess = config.getInt("future.cache.expire.after.access")


  /**
   * create serializer that knows how to convert given snapshotEdge into kvs: Seq[SKeyValue]
   * so we can store this kvs.
   * @param snapshotEdge
   * @return
   */
  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge) = new SnapshotEdgeSerializable(snapshotEdge)

  /**
   * create serializer that knows how to convert given indexEdge into kvs: Seq[SKeyValue]
   * @param indexedEdge
   * @return
   */
  def indexEdgeSerializer(indexedEdge: IndexEdge) = new IndexEdgeSerializable(indexedEdge)

  /**
   * create serializer that knows how to convert given vertex into kvs: Seq[SKeyValue]
   * @param vertex
   * @return
   */
  def vertexSerializer(vertex: Vertex) = new VertexSerializable(vertex)

  /**
   * create deserializer that can parse stored CanSKeyValue into snapshotEdge.
   * note that each storage implementation should implement implicit type class CanSKeyValue
    * */
  val snapshotEdgeDeserializer = new SnapshotEdgeDeserializable

  /** create deserializer that can parse stored CanSKeyValue into indexEdge. */
  val indexEdgeDeserializer = new IndexEdgeDeserializable

  /** create deserializer that can parser stored CanSKeyValue into vertex. */
  val vertexDeserializer = new VertexDeserializable


  
  /**
   * decide how to store given SKeyValue into storage.
   * @param kv
   * @param withWait
   * @return
   */
  def writeToStorage(kv: SKeyValue, withWait: Boolean): Future[Boolean]

  /**
   * decide how to apply given edges(indexProps values + Map(_count -> countVal)) into storage.
   * @param edges
   * @param withWait
   * @return
   */
  def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]]

  /**
   * this method need to be called when client shutdown. this is responsible to cleanUp the resources
   * such as client into storage.
   */
  def flush(): Unit

  /**
   * create table on storage.
   * if storage implementation does not support namespace or table, then there is nothing to be done
   * @param zkAddr
   * @param tableName
   * @param cfs
   * @param regionMultiplier
   * @param ttl
   * @param compressionAlgorithm
   */
  def createTable(zkAddr: String,
                  tableName: String,
                  cfs: List[String],
                  regionMultiplier: Int,
                  ttl: Option[Int],
                  compressionAlgorithm: String): Unit

  /**
   * responsible for consistency guarantee.
   * we have optimistic concurrency control example on AsynchbaseStorage.
   * each storage is responsible to resolve conflict on sanme snapshotEdge in this method.
   * note that this method can throw few exceptions.
   * Basic requirement is following.
   * 1. Fetch snapshotEdge.
   * 2. Build modification based on indexEdge(request) and fetched snapshotEdge
   * 3. Atomically mutate theses modifications into storage.
   *
   * we can use either pessimistic or optimistic concurrency control.
   * one example would be storage is RDBMS and use transaction support from storage.
   *
   * other example would be use compareAndSet and store lock on storage to guarantee write-write conflict can abort.
   * when conflict found, implementation can throw PartialFailureException with failed request and statusCode.
   *
   * retry logic will call this method for MaxRetryNum until this return Future[true].
   *
   * note that fetchSnapshotEdge can be failed because of timeout also so retry logic will catch this exception too.
   *
   * other than FetchTimeoutException, PartialFailureException retry logic will not retry even though commitUpdate return false.
   * so be sure throw proper exception according to failure cases.
   * 
   * @param edges
   * @param statusCode
   * @return
   */
  def commitUpdate(edges: Seq[Edge], statusCode: Byte): Future[Boolean]

  /**
   * fetch IndexEdges for given request from storage.
   * @param request
   * @return
   */
  def fetchIndexEdgeKeyValues(request: AnyRef): Future[Seq[SKeyValue]]

  /**
   * fetch SnapshotEdge for given request from storage.
   * also storage datatype should be converted into SKeyValue.
   * note that return type is Sequence rather than single SKeyValue for simplicity,
   * even though there is assertions sequence.length == 1.
   * @param request
   * @return
   */
  def fetchSnapshotEdgeKeyValues(request: AnyRef): Future[Seq[SKeyValue]]

  /**
   * build proper request which is specific into storage to call fetchIndexEdgeKeyValues or fetchSnapshotEdgeKeyValues.
   * for example, Asynchbase use GetRequest, Scanner so this method is responsible to build
   * client request(GetRequest, Scanner) based on user provided query.
   * @param queryRequest
   * @return
   */
  def buildRequest(queryRequest: QueryRequest): AnyRef

  /**
   * fetch IndexEdges for given queryParam in queryRequest.
   * this expect previous step starting score to propagate score into next step.
   * also parentEdges is necessary to return full bfs tree when query require it.
   *
   * note that return type is general type.
   * for example, currently we wanted to use Asynchbase
   * so single I/O return type should be Deferred[T].
   *
   * if we use native hbase client, then this return type can be Future[T] or just T.
   * @param queryRequest
   * @param prevStepScore
   * @param isInnerCall
   * @param parentEdges
   * @return
   */
  def fetch(queryRequest: QueryRequest,
            prevStepScore: Double,
            isInnerCall: Boolean,
            parentEdges: Seq[EdgeWithScore]): R


  /**
   * responsible to fire parallel fetch call into storage and create future that will return merged result.
   * @param queryRequestWithScoreLs
   * @param prevStepEdges
   * @return
   */
  def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]]




  /** Public Interface */
  def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = {
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
      val queryParam = QueryParam.Empty
      val q = Query.toQuery(Seq(vertex), queryParam)
      val queryRequest = QueryRequest(q, stepIdx = -1, vertex, queryParam)
      fetchIndexEdgeKeyValues(buildRequest(queryRequest)).map { kvs =>
        fromResult(queryParam, kvs, vertex.serviceColumn.schemaVersion)
      } recoverWith { case ex: Throwable =>
        Future.successful(None)
      }
    }

    Future.sequence(futures).map { result => result.toList.flatten }
  }

  def mutateElements(elements: Seq[GraphElement],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {

    val edgeBuffer = ArrayBuffer[Edge]()
    val vertexBuffer = ArrayBuffer[Vertex]()

    elements.foreach {
      case e: Edge => edgeBuffer += e
      case v: Vertex => vertexBuffer += v
      case any@_ => logger.error(s"Unknown type: ${any}")
    }

    val edgeFuture = mutateEdges(edgeBuffer, withWait)
    val vertexFuture = mutateVertices(vertexBuffer, withWait)

    val graphFuture = for {
      edgesMutated <- edgeFuture
      verticesMutated <- vertexFuture
    } yield edgesMutated ++ verticesMutated

    graphFuture
  }


  def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = {
    val edgeFuture =
      if (edge.op == GraphUtil.operations("deleteAll")) {
        deleteAllAdjacentEdges(Seq(edge.srcVertex), Seq(edge.label), edge.labelWithDir.dir, edge.ts)
      } else {
        val strongConsistency = edge.label.consistencyLevel == "strong"
        if (edge.op == GraphUtil.operations("delete") && !strongConsistency) {
          val zkQuorum = edge.label.hbaseZkAddr
          val (_, edgeUpdate) = Edge.buildDeleteBulk(None, edge)
          val mutations =
            indexedEdgeMutations(edgeUpdate) ++ snapshotEdgeMutations(edgeUpdate) ++ increments(edgeUpdate)
          writeAsyncSimple(zkQuorum, mutations, withWait)
        } else {
          mutateEdgesInner(Seq(edge), strongConsistency, withWait)
        }
      }

    val vertexFuture = writeAsyncSimple(edge.label.hbaseZkAddr, buildVertexPutsAsync(edge), withWait)

    Future.sequence(Seq(edgeFuture, vertexFuture)).map(_.forall(identity))
  }

  def mutateEdges(_edges: Seq[Edge], withWait: Boolean): Future[Seq[Boolean]] = {
    val grouped = _edges.groupBy { edge => (edge.label, edge.srcVertex.innerId, edge.tgtVertex.innerId) } toSeq

    val mutateEdges = grouped.map { case ((_, _, _), edgeGroup) =>
      val (deleteAllEdges, edges) = edgeGroup.partition(_.op == GraphUtil.operations("deleteAll"))

      // DeleteAll first
      val deleteAllFutures = deleteAllEdges.map { edge =>
        deleteAllAdjacentEdges(Seq(edge.srcVertex), Seq(edge.label), edge.labelWithDir.dir, edge.ts)
      }

      // After deleteAll, process others
      lazy val mutateEdgeFutures = edges.toList match {
        case head :: tail =>
          val strongConsistency = edges.head.label.consistencyLevel == "strong"
          if (strongConsistency) {
            val edgeFuture = mutateEdgesInner(edges, strongConsistency, withWait)

            //TODO: decide what we will do on failure on vertex put
            val puts = buildVertexPutsAsync(head)
            val vertexFuture = writeAsyncSimple(head.label.hbaseZkAddr, puts, withWait)
            Seq(edgeFuture, vertexFuture)
          } else {
            edges.map { edge => mutateEdge(edge, withWait = withWait) }
          }
        case Nil => Nil
      }

      val composed = for {
        deleteRet <- Future.sequence(deleteAllFutures)
        mutateRet <- Future.sequence(mutateEdgeFutures)
      } yield deleteRet ++ mutateRet

      composed.map(_.forall(identity))
    }

    Future.sequence(mutateEdges)
  }



  def mutateEdgesInner(edges: Seq[Edge], checkConsistency: Boolean, withWait: Boolean): Future[Boolean] = {
    if (!checkConsistency) {
      val zkQuorum = edges.head.label.hbaseZkAddr
      val futures = edges.map { edge =>
        val (_, edgeUpdate) = Edge.buildOperation(None, Seq(edge))
        val mutations =
          indexedEdgeMutations(edgeUpdate) ++
            snapshotEdgeMutations(edgeUpdate) ++
            increments(edgeUpdate)
        writeAsyncSimple(zkQuorum, mutations, withWait)
      }
      Future.sequence(futures).map { rets => rets.forall(identity) }
    } else {
      def retry(tryNum: Int)(edges: Seq[Edge], statusCode: Byte): Future[Boolean] = {
        if (tryNum >= MaxRetryNum) {
          edges.foreach { edge =>
            logger.error(s"commit failed after $MaxRetryNum\n${edge.toLogString}")
            ExceptionHandler.enqueue(ExceptionHandler.toKafkaMessage(element = edge))
          }
          Future.successful(false)
        } else {
          val future = commitUpdate(edges, statusCode)

          future.onSuccess {
            case success =>
              logger.debug(s"Finished. [$tryNum]\n${edges.head.toLogString}\n")
          }
          future recoverWith {
            case FetchTimeoutException(retryEdge) =>
              logger.info(s"[Try: $tryNum], Fetch fail.\n${retryEdge}")
              retry(tryNum + 1)(edges, statusCode)

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
              retry(tryNum + 1)(Seq(retryEdge), failedStatusCode)
            case ex: Exception =>
              logger.error("Unknown exception", ex)
              Future.successful(false)
          }
        }
      }
      retry(1)(edges, 0)
    }
  }

  def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = {
    if (vertex.op == GraphUtil.operations("delete")) {
      writeAsyncSimple(vertex.hbaseZkAddr,
        vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete)), withWait)
    } else if (vertex.op == GraphUtil.operations("deleteAll")) {
      logger.info(s"deleteAll for vertex is truncated. $vertex")
      Future.successful(true) // Ignore withWait parameter, because deleteAll operation may takes long time
    } else {
      writeAsyncSimple(vertex.hbaseZkAddr, buildPutsAll(vertex), withWait)
    }
  }

  def mutateVertices(vertices: Seq[Vertex],
                     withWait: Boolean = false): Future[Seq[Boolean]] = {
    val futures = vertices.map { vertex => mutateVertex(vertex, withWait) }
    Future.sequence(futures)
  }


  /** Delete All */
  protected def deleteAllFetchedEdgesAsyncOld(queryRequest: QueryRequest,
                                              queryResult: QueryResult,
                                              requestTs: Long,
                                              retryNum: Int): Future[Boolean] = {
    val queryParam = queryRequest.queryParam
    val zkQuorum = queryParam.label.hbaseZkAddr
    val futures = for {
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
    } yield {
        /** reverted direction */
        val reversedIndexedEdgesMutations = edge.duplicateEdge.edgesWithIndex.flatMap { indexEdge =>
          indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            buildIncrementsAsync(indexEdge, -1L)
        }
        val reversedSnapshotEdgeMutations = snapshotEdgeSerializer(edge.toSnapshotEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put))
        val forwardIndexedEdgeMutations = edge.edgesWithIndex.flatMap { indexEdge =>
          indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete)) ++
            buildIncrementsAsync(indexEdge, -1L)
        }
        val mutations = reversedIndexedEdgesMutations ++ reversedSnapshotEdgeMutations ++ forwardIndexedEdgeMutations
        writeAsyncSimple(zkQuorum, mutations, withWait = true)
      }

    Future.sequence(futures).map { rets => rets.forall(identity) }
  }

  protected def buildEdgesToDelete(queryRequestWithResultLs: QueryRequestWithResult, requestTs: Long): QueryResult = {
    val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResultLs).get
    val edgeWithScoreLs = queryResult.edgeWithScoreLs.filter { edgeWithScore =>
      (edgeWithScore.edge.ts < requestTs) && !edgeWithScore.edge.isDegree
    }.map { edgeWithScore =>
      val label = queryRequest.queryParam.label
      val (newOp, newVersion, newPropsWithTs) = label.consistencyLevel match {
        case "strong" =>
          val _newPropsWithTs = edgeWithScore.edge.propsWithTs ++
            Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(requestTs, requestTs, label.schemaVersion))
          (GraphUtil.operations("delete"), requestTs, _newPropsWithTs)
        case _ =>
          val oldEdge = edgeWithScore.edge
          (oldEdge.op, oldEdge.version, oldEdge.propsWithTs)
      }

      val copiedEdge =
        edgeWithScore.edge.copy(op = newOp, version = newVersion, propsWithTs = newPropsWithTs)

      val edgeToDelete = edgeWithScore.copy(edge = copiedEdge)
//      logger.debug(s"delete edge from deleteAll: ${edgeToDelete.edge.toLogString}")
      edgeToDelete
    }

    queryResult.copy(edgeWithScoreLs = edgeWithScoreLs)
  }

  protected def deleteAllFetchedEdgesLs(queryRequestWithResultLs: Seq[QueryRequestWithResult], requestTs: Long): Future[(Boolean, Boolean)] = {
    val queryResultLs = queryRequestWithResultLs.map(_.queryResult)
    queryResultLs.foreach { queryResult =>
      if (queryResult.isFailure) throw new RuntimeException("fetched result is fallback.")
    }
    val futures = for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, _) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      deleteQueryResult = buildEdgesToDelete(queryRequestWithResult, requestTs)
      if deleteQueryResult.edgeWithScoreLs.nonEmpty
    } yield {
        val label = queryRequest.queryParam.label
        label.schemaVersion match {
          case HBaseType.VERSION3 | HBaseType.VERSION4 =>
            if (label.consistencyLevel == "strong") {
              /**
               * read: snapshotEdge on queryResult = O(N)
               * write: N x (relatedEdges x indices(indexedEdge) + 1(snapshotEdge))
               */
              mutateEdges(deleteQueryResult.edgeWithScoreLs.map(_.edge), withWait = true).map(_.forall(identity))
            } else {
              deleteAllFetchedEdgesAsyncOld(queryRequest, deleteQueryResult, requestTs, MaxRetryNum)
            }
          case _ =>

            /**
             * read: x
             * write: N x ((1(snapshotEdge) + 2(1 for incr, 1 for delete) x indices)
             */
            deleteAllFetchedEdgesAsyncOld(queryRequest, deleteQueryResult, requestTs, MaxRetryNum)
        }
      }

    if (futures.isEmpty) {
      // all deleted.
      Future.successful(true -> true)
    } else {
      Future.sequence(futures).map { rets => false -> rets.forall(identity) }
    }
  }

  protected def fetchAndDeleteAll(query: Query, requestTs: Long): Future[(Boolean, Boolean)] = {
    val future = for {
      queryRequestWithResultLs <- getEdges(query)
      (allDeleted, ret) <- deleteAllFetchedEdgesLs(queryRequestWithResultLs, requestTs)
    } yield {
//        logger.debug(s"fetchAndDeleteAll: ${allDeleted}, ${ret}")
        (allDeleted, ret)
      }

    Extensions.retryOnFailure(MaxRetryNum) {
      future
    } {
      logger.error(s"fetch and deleteAll failed.")
      (true, false)
    }

  }

  def deleteAllAdjacentEdges(srcVertices: Seq[Vertex],
                             labels: Seq[Label],
                             dir: Int,
                             ts: Long): Future[Boolean] = {

    def enqueueLogMessage() = {
      val kafkaMessages = for {
        vertice <- srcVertices
        id = vertice.innerId.toIdString()
        label <- labels
      } yield {
          val tsv = Seq(ts, "deleteAll", "e", id, id, label.label, "{}", GraphUtil.fromOp(dir.toByte)).mkString("\t")
          val topic = ExceptionHandler.failTopic
          val kafkaMsg = KafkaMessage(new ProducerRecord[Key, Val](topic, null, tsv))
          kafkaMsg
        }

      ExceptionHandler.enqueues(kafkaMessages)
    }

    val requestTs = ts
    val queryParams = for {
      label <- labels
    } yield {
        val labelWithDir = LabelWithDirection(label.id.get, dir)
        QueryParam(labelWithDir).limit(0, DeleteAllFetchSize).duplicatePolicy(Option(Query.DuplicatePolicy.Raw))
      }

    val step = Step(queryParams.toList)
    val q = Query(srcVertices, Vector(step))

    //    Extensions.retryOnSuccessWithBackoff(MaxRetryNum, Random.nextInt(MaxBackOff) + 1) {
    val retryFuture = Extensions.retryOnSuccess(MaxRetryNum) {
      fetchAndDeleteAll(q, requestTs)
    } { case (allDeleted, deleteSuccess) =>
      allDeleted && deleteSuccess
    }.map { case (allDeleted, deleteSuccess) => allDeleted && deleteSuccess }

    retryFuture onFailure {
      case ex =>
        logger.error(s"[Error]: deleteAllAdjacentEdges failed.")
        enqueueLogMessage()
    }

    retryFuture
  }

  /** End Of Delete All */




  /** Parsing Logic: parse from kv from Storage into Edge */

  def toEdge[K: CanSKeyValue](kv: K,
                              queryParam: QueryParam,
                              cacheElementOpt: Option[IndexEdge],
                              parentEdges: Seq[EdgeWithScore]): Option[Edge] = {
//        logger.debug(s"toEdge: $kv")
    try {
      val indexEdge = indexEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)

      Option(indexEdge.toEdge.copy(parentEdges = parentEdges))
    } catch {
      case ex: Exception =>
        logger.error(s"Fail on toEdge: ${kv.toString}, ${queryParam}", ex)
        None
    }
  }

  def toSnapshotEdge[K: CanSKeyValue](kv: K,
                                      queryParam: QueryParam,
                                      cacheElementOpt: Option[SnapshotEdge] = None,
                                      isInnerCall: Boolean,
                                      parentEdges: Seq[EdgeWithScore]): Option[Edge] = {
//        logger.debug(s"SnapshottoEdge: $kv")
    val snapshotEdge = snapshotEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, cacheElementOpt)

    if (isInnerCall) {
      val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
      if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
      else None
    } else {
      if (Edge.allPropsDeleted(snapshotEdge.props)) None
      else {
        val edge = snapshotEdge.toEdge.copy(parentEdges = parentEdges)
        if (queryParam.where.map(_.filter(edge)).getOrElse(true)) Option(edge)
        else None
      }
    }
  }

  def toEdges[K: CanSKeyValue](kvs: Seq[K],
                               queryParam: QueryParam,
                               prevScore: Double = 1.0,
                               isInnerCall: Boolean,
                               parentEdges: Seq[EdgeWithScore]): Seq[EdgeWithScore] = {
    if (kvs.isEmpty) Seq.empty
    else {
      val first = kvs.head
      val kv = first
      val cacheElementOpt =
        if (queryParam.isSnapshotEdge) None
        else Option(indexEdgeDeserializer.fromKeyValues(queryParam, Seq(kv), queryParam.label.schemaVersion, None))

      for {
        kv <- kvs
        edge <-
        if (queryParam.isSnapshotEdge) toSnapshotEdge(kv, queryParam, None, isInnerCall, parentEdges)
        else toEdge(kv, queryParam, cacheElementOpt, parentEdges)
      } yield {
        //TODO: Refactor this.
        val currentScore =
          queryParam.scorePropagateOp match {
            case "plus" => edge.rank(queryParam.rank) + prevScore
            case _ => edge.rank(queryParam.rank) * prevScore
          }
        EdgeWithScore(edge, currentScore)
      }
    }
  }

  /** End Of Parse Logic */

  /** methods for consistency */
  protected def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[SKeyValue], withWait: Boolean): Future[Boolean] = {
    if (elementRpcs.isEmpty) {
      Future.successful(true)
    } else {
      val futures = elementRpcs.map { rpc => writeToStorage(rpc, withWait) }
      Future.sequence(futures).map(_.forall(identity))
    }
  }

  case class PartialFailureException(edge: Edge, statusCode: Byte, failReason: String) extends Exception

  protected def toRequestEdge(queryRequest: QueryRequest): Edge = {
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
    Edge(srcV, tgtV, labelWithDir, propsWithTs = propsWithTs)
  }



  protected def fetchSnapshotEdge(edge: Edge): Future[(QueryParam, Option[Edge], Option[SKeyValue])] = {
    val labelWithDir = edge.labelWithDir
    val queryParam = QueryParam(labelWithDir)
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(edge.tgtVertex.innerId))
    val q = Query.toQuery(Seq(edge.srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, edge.srcVertex, _queryParam)

    fetchSnapshotEdgeKeyValues(buildRequest(queryRequest)).map { kvs =>
//    fetchIndexEdgeKeyValues(buildRequest(queryRequest)).map { kvs =>
      val (edgeOpt, kvOpt) =
        if (kvs.isEmpty) (None, None)
        else {
          val _edgeOpt = toEdges(kvs, queryParam, 1.0, isInnerCall = true, parentEdges = Nil).headOption.map(_.edge)
          val _kvOpt = kvs.headOption
          (_edgeOpt, _kvOpt)
        }
      (queryParam, edgeOpt, kvOpt)
    } recoverWith { case ex: Throwable =>
      logger.error(s"fetchQueryParam failed. fallback return.", ex)
      throw new FetchTimeoutException(s"${edge.toLogString}")
    }
  }

  protected def fetchStep(orgQuery: Query, queryRequestWithResultsLs: Seq[QueryRequestWithResult]): Future[Seq[QueryRequestWithResult]] = {
    if (queryRequestWithResultsLs.isEmpty) Future.successful(Nil)
    else {
      val queryRequest = queryRequestWithResultsLs.head.queryRequest
      val q = orgQuery
      val queryResultsLs = queryRequestWithResultsLs.map(_.queryResult)

      val stepIdx = queryRequest.stepIdx + 1

      val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
      val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
      val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
      val step = q.steps(stepIdx)
      val alreadyVisited =
        if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
        else Graph.alreadyVisitedVertices(queryResultsLs)

      val groupedBy = queryResultsLs.flatMap { queryResult =>
        queryResult.edgeWithScoreLs.map { case edgeWithScore =>
          edgeWithScore.edge.tgtVertex -> edgeWithScore
        }
      }.groupBy { case (vertex, edgeWithScore) => vertex }

      val groupedByFiltered = for {
        (vertex, edgesWithScore) <- groupedBy
        aggregatedScore = edgesWithScore.map(_._2.score).sum if aggregatedScore >= prevStepThreshold
      } yield vertex -> aggregatedScore

      val prevStepTgtVertexIdEdges = for {
        (vertex, edgesWithScore) <- groupedBy
      } yield vertex.id -> edgesWithScore.map { case (vertex, edgeWithScore) => edgeWithScore }

      val nextStepSrcVertices = if (prevStepLimit >= 0) {
        groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
      } else {
        groupedByFiltered.toSeq
      }

      val queryRequests = for {
        (vertex, prevStepScore) <- nextStepSrcVertices
        queryParam <- step.queryParams
      } yield (QueryRequest(q, stepIdx, vertex, queryParam), prevStepScore)

      Graph.filterEdges(fetches(queryRequests, prevStepTgtVertexIdEdges), alreadyVisited)(ec)
    }
  }

  protected def fetchStepFuture(orgQuery: Query, queryRequestWithResultLsFuture: Future[Seq[QueryRequestWithResult]]): Future[Seq[QueryRequestWithResult]] = {
    for {
      queryRequestWithResultLs <- queryRequestWithResultLsFuture
      ret <- fetchStep(orgQuery, queryRequestWithResultLs)
    } yield ret
  }

  def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = {
    val fallback = {
      val queryRequest = QueryRequest(query = q, stepIdx = 0, q.vertices.head, queryParam = QueryParam.Empty)
      Future.successful(q.vertices.map(v => QueryRequestWithResult(queryRequest, QueryResult())))
    }
    Try {

      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        fallback
      } else {
        // current stepIdx = -1
        val startQueryResultLs = QueryResult.fromVertices(q)
        q.steps.foldLeft(Future.successful(startQueryResultLs)) { case (acc, step) =>
            fetchStepFuture(q, acc)
//          fetchStepFuture(q, acc).map { stepResults =>
//            step.queryParams.zip(stepResults).foreach { case (qParam, queryRequestWithResult)  =>
//              val cursor = Base64.getEncoder.encodeToString(queryRequestWithResult.queryResult.tailCursor)
//              qParam.cursorOpt = Option(cursor)
//            }
//            stepResults
//          }
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = {
    val ts = System.currentTimeMillis()
    val futures = for {
      (srcVertex, tgtVertex, queryParam) <- params
      propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(ts, ts, queryParam.label.schemaVersion))
      edge = Edge(srcVertex, tgtVertex, queryParam.labelWithDir, propsWithTs = propsWithTs)
    } yield {
        fetchSnapshotEdge(edge).map { case (queryParam, edgeOpt, kvOpt) =>
          val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(edge.tgtVertex.innerId))
          val q = Query.toQuery(Seq(edge.srcVertex), _queryParam)
          val queryRequest = QueryRequest(q, 0, edge.srcVertex, _queryParam)
          val queryResult = QueryResult(edgeOpt.toSeq.map(e => EdgeWithScore(e, 1.0)))
          QueryRequestWithResult(queryRequest, queryResult)
        }
      }

    Future.sequence(futures)
  }



  @tailrec
  final def randomInt(sampleNumber: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
    if (range < sampleNumber || set.size == sampleNumber) set
    else randomInt(sampleNumber, range, set + Random.nextInt(range))
  }

  protected def sample(queryRequest: QueryRequest, edges: Seq[EdgeWithScore], n: Int): Seq[EdgeWithScore] = {
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
  /** end of query */

  /** Mutation Builder */


  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] = {
    val deleteMutations = edgeMutate.edgesToDelete.flatMap { indexEdge =>
      indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Delete))
    }
    val insertMutations = edgeMutate.edgesToInsert.flatMap { indexEdge =>
      indexEdgeSerializer(indexEdge).toKeyValues.map(_.copy(operation = SKeyValue.Put))
    }

    deleteMutations ++ insertMutations
  }

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    edgeMutate.newSnapshotEdge.map(e => snapshotEdgeSerializer(e).toKeyValues).getOrElse(Nil)

  def increments(edgeMutate: EdgeMutate): Seq[SKeyValue] =
    (edgeMutate.edgesToDelete.isEmpty, edgeMutate.edgesToInsert.isEmpty) match {
      case (true, true) =>

        /** when there is no need to update. shouldUpdate == false */
        List.empty
      case (true, false) =>

        /** no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeMutate.edgesToInsert.flatMap { e => buildIncrementsAsync(e) }
      case (false, true) =>

        /** no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeMutate.edgesToDelete.flatMap { e => buildIncrementsAsync(e, -1L) }
      case (false, false) =>

        /** update on existing edges so no change on degree */
        List.empty
    }

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.props ++ Map(LabelMeta.degreeSeq -> InnerVal.withLong(amount, indexedEdge.schemaVer))
    val _indexedEdge = indexedEdge.copy(props = newProps)
    indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment))
  }

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[SKeyValue] = {
    val newProps = indexedEdge.props ++ Map(LabelMeta.countSeq -> InnerVal.withLong(amount, indexedEdge.schemaVer))
    val _indexedEdge = indexedEdge.copy(props = newProps)
    indexEdgeSerializer(_indexedEdge).toKeyValues.map(_.copy(operation = SKeyValue.Increment))
  }
  def buildDeleteBelongsToId(vertex: Vertex): Seq[SKeyValue] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    val kv = kvs.head
    vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(Vertex.toPropKey(id)), operation = SKeyValue.Delete)
    }
  }

  def buildVertexPutsAsync(edge: Edge): Seq[SKeyValue] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      vertexSerializer(edge.srcForVertex).toKeyValues ++ vertexSerializer(edge.tgtForVertex).toKeyValues

  def buildPutsAll(vertex: Vertex): Seq[SKeyValue] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Delete))
      case _ => vertexSerializer(vertex).toKeyValues.map(_.copy(operation = SKeyValue.Put))
    }
  }
}
