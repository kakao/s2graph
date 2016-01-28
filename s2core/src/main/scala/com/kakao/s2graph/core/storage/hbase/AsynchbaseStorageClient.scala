package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core.storage.hbase.AsynchbaseStorage
import com.kakao.s2graph.core.utils.{logger, Extensions}
import com.typesafe.config.Config
import org.hbase.async._
import scala.concurrent.{ExecutionContext, Future}

class AsynchbaseStorageClient(config: Config)(implicit ec: ExecutionContext)
  extends StorageClient[HBaseRpc](config) {

  var client: HBaseClient = AsynchbaseStorage.makeClient(config)
  import Extensions.DeferOps

  val ex = new RuntimeException("wrong rpc type.")

  override def delete(rpc: HBaseRpc): Future[Boolean] =
    rpc match {
      case d: DeleteRequest => client.delete(d).withCallback(_ => true).recoverWith { ex =>
        logger.error(s"mutation failed. $rpc", ex)
        false
      } toFuture
      case _ => Future.failed(ex)
    }

  override def compareAndSet(rpc: HBaseRpc, expected: Array[Byte]): Future[Boolean] = {
    rpc match {
      case p: PutRequest => client.compareAndSet(p, expected).withCallback(_ => true).recoverWith { ex =>
        logger.error(s"mutation failed. $rpc", ex)
        false
      } toFuture
      case _ => Future.failed(ex)
    }
  }

  override def put(rpc: HBaseRpc): Future[Boolean] =
    rpc match {
      case p: PutRequest => client.put(p).withCallback(_ => true).recoverWith { ex =>
        logger.error(s"mutation failed. $rpc", ex)
        false
      } toFuture
      case _ => Future.failed(ex)
    }

  override def increment(rpc: HBaseRpc): Future[Long] =
    rpc match {
      case i: AtomicIncrementRequest => client.bufferAtomicIncrement(i).toFuture.map(_.toLong)
      case _ => Future.failed(ex)
    }

}
