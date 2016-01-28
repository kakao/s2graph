//package com.kakao.s2graph.core.storage.hbase
//
//import com.kakao.s2graph.core.storage.{SKeyValue, MutationBuilder}
//import org.apache.hadoop.hbase.util.Bytes
//import org.hbase.async.{DeleteRequest, AtomicIncrementRequest, PutRequest, HBaseRpc}
//import scala.collection.Seq
//import scala.concurrent.ExecutionContext
//
//class AsynchbaseMutationBuilder(storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
//  extends MutationBuilder[HBaseRpc](storage) {
//
//  def put(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
//    kvs.map { kv => new PutRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.value, kv.timestamp) }
//
//  def increment(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
//    kvs.map { kv => new AtomicIncrementRequest(kv.table, kv.row, kv.cf, kv.qualifier, Bytes.toLong(kv.value)) }
//
//  def delete(kvs: Seq[SKeyValue]): Seq[HBaseRpc] =
//    kvs.map { kv =>
//      if (kv.qualifier == null) new DeleteRequest(kv.table, kv.row, kv.cf, kv.timestamp)
//      else new DeleteRequest(kv.table, kv.row, kv.cf, kv.qualifier, kv.timestamp)
//    }
//
//}
