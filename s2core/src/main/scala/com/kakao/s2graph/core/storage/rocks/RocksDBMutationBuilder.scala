//package com.kakao.s2graph.core.storage.rocks
//
//import com.kakao.s2graph.core._
//import com.kakao.s2graph.core.storage.{SKeyValue, MutationBuilder}
//import scala.concurrent.ExecutionContext
//
//class RocksDBMutationBuilder(storage: RocksDBStorage)(implicit ec: ExecutionContext)
//  extends MutationBuilder[SKeyValue](storage) {
//  /** operation that needs to be supported by backend persistent storage system */
//  override def put(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs
//
//  override def increment(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs
//
//  override def delete(kvs: Seq[SKeyValue]): Seq[SKeyValue] = kvs
//}
