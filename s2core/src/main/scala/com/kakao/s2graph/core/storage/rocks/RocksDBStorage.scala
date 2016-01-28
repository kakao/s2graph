//package com.kakao.s2graph.core.storage.rocks
//
//import com.google.common.cache.Cache
//import com.kakao.s2graph.core.mysqls.Label
//import com.kakao.s2graph.core._
//import com.kakao.s2graph.core.storage.hbase._
//import com.kakao.s2graph.core.storage.{SKeyValue, StorageDeserializable, StorageSerializable, Storage}
//import com.kakao.s2graph.core.utils.logger
//import com.typesafe.config.Config
//import org.rocksdb.{RocksDBException, RocksDB, Options}
//
//import scala.concurrent.{Future, ExecutionContext}
//
//
//class RocksDBStorage(override val config: Config)(implicit ec: ExecutionContext) extends Storage(config) {
//  RocksDB.loadLibrary()
//
//  val options = new Options().setCreateIfMissing(true)
//  var db: RocksDB = null
//  val queryBuilder = new RocksDBQueryBuilder(this)(ec)
//  val mutationBuilder = new RocksDBMutationBuilder(this)(ec)
//
//  try {
//    // a factory method that returns a RocksDB instance
//    db = RocksDB.open(options, "/tmp/rocks")
//
//    // do something
//  } catch {
//    case e: RocksDBException =>
//
//  }
//
//  override def cacheOpt: Option[Cache[Integer, Seq[QueryResult]]] = None
//
//  override def createTable(zkAddr: String, tableName: String, cfs: List[String], regionMultiplier: Int, ttl: Option[Int], compressionAlgorithm: String): Unit = {
//
//  }
//
//  // Serializer/Deserializer
//  override def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): StorageSerializable[SnapshotEdge] = new SnapshotEdgeSerializable(snapshotEdge)
//
//  override def snapshotEdgeDeserializer: StorageDeserializable[SnapshotEdge] = new SnapshotEdgeDeserializable
//
//  override def vertexSerializer(vertex: Vertex): StorageSerializable[Vertex] = new VertexSerializable(vertex)
//
//  override def indexEdgeDeserializer: StorageDeserializable[IndexEdge] = new IndexEdgeDeserializable
//
//  override def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = Future {
//    Seq.empty
//  }
//
//  override def flush(): Unit = {
//    if (db != null) db.close()
//    options.dispose()
//  }
//
//  override def vertexDeserializer: StorageDeserializable[Vertex] = new VertexDeserializable
//
//  override def vertexCacheOpt: Option[Cache[Integer, Option[Vertex]]] = None
//
//  override def indexEdgeSerializer(indexedEdge: IndexEdge): StorageSerializable[IndexEdge] = new IndexEdgeSerializable(indexedEdge)
//
//  // Interface
//  override def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = queryBuilder.getEdges(q)
//
//  override def deleteAllAdjacentEdges(srcVertices: Seq[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] = Future {
//    true
//  }
//
//  override def mutateEdges(edges: Seq[Edge], withWait: Boolean): Future[Seq[Boolean]] = Future.sequence(edges.map { edge => mutateEdge(edge, withWait) })
//
//  override def incrementCounts(edges: Seq[Edge], withWait: Boolean): Future[Seq[(Boolean, Long)]] = Future {
//    Seq.empty
//  }
//
//  override def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = Future {
//    true
//  }
//
//  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = Future {
//    Seq.empty
//  }
//
//  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = Future {
////    val edgeFuture =
////      if (edge.op == GraphUtil.operations("deleteAll")) {
////        deleteAllAdjacentEdges(Seq(edge.srcVertex), Seq(edge.label), edge.labelWithDir.dir, edge.ts)
////      } else {
////        val strongConsistency = edge.label.consistencyLevel == "strong"
//////        if (edge.op == GraphUtil.operations("delete") && !strongConsistency) {
////          val zkQuorum = edge.label.hbaseZkAddr
////          val (_, edgeUpdate) = Edge.buildDeleteBulk(None, edge)
////          val mutations =
////            mutationBuilder.indexedEdgeMutations(edgeUpdate) ++
////              mutationBuilder.snapshotEdgeMutations(edgeUpdate) ++
////              mutationBuilder.increments(edgeUpdate)
////
////          mutations.foreach { kv => db.put(kv.row, kv.value) }
//////        } else {
//////          mutateEdgesInner(Seq(edge), strongConsistency, withWait)(Edge.buildOperation)
//////        }
////      }
////
////    val vertexFuture = writeAsyncSimple(edge.label.hbaseZkAddr,
////      mutationBuilder.buildVertexPutsAsync(edge), withWait)
////
////    Future.sequence(Seq(edgeFuture, vertexFuture)).map(_.forall(identity))
//
//    val kvs = edge.edgesWithIndex.flatMap { indexEdge =>
//      indexEdgeSerializer(indexEdge).toKeyValues ++ snapshotEdgeSerializer(edge.toSnapshotEdge).toKeyValues
//    } ++ vertexSerializer(edge.srcVertex).toKeyValues ++ vertexSerializer(edge.tgtVertex).toKeyValues
//    for {
//      kv <- kvs
//    } {
//      logger.debug(s"Key: ${kv.row.toList}, Value: ${kv.value.toList}")
//      db.put(kv.row, kv.value)
//    }
//    true
//  }
//
//}
