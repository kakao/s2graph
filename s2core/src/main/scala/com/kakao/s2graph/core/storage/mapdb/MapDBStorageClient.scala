package com.kakao.s2graph.core.storage.mapdb

import java.util

import com.kakao.s2graph.core.utils.logger
import com.stumbleupon.async.Deferred
import org.hbase.async._
import org.mapdb.{DB, DBMaker}

import scala.collection.JavaConversions._
import scala.collection.mutable

case class HashingKey(_1: Array[Byte], _2: Array[Byte]) {
  val asString = {
    val concat = new Array[Byte](_1.length + _2.length)
    System.arraycopy(_1, 0, concat, 0, _1.length)
    System.arraycopy(_2, 0, concat, _1.length, _2.length)
    new String(concat)
  }
}

case class IndexingKey(underlying: Array[Byte]) extends Comparable[IndexingKey] {
  override def compareTo(o: IndexingKey): Int = Bytes.memcmp(underlying, o.underlying)
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: IndexingKey if other != null =>
        Bytes.equals(underlying, other.underlying)
      case _ => false
    }
  }

  override def toString: String = underlying.mkString("[", ", ", "]")
}

class MapDBStorageClient {

  private val underlying = new mutable.HashMap[String, DB]

  private def getTreeMap(tableName: Array[Byte], key: String) = {
    val table = underlying.getOrElseUpdate(new String(tableName), DBMaker.memoryDB().make())
    table.treeMap[IndexingKey, (Long, Array[Byte])](key)
  }

  private def deleteTreeMap(table: Array[Byte], key: String) = {
    underlying.get(new String(table)).foreach { table =>
      table.delete(key)
    }
  }

  def get(req: GetRequest): Deferred[util.ArrayList[KeyValue]] = {
    logger.debug("get: " + req.toString)
    val kvs = new util.ArrayList[KeyValue]()
    val indexedMap = getTreeMap(req.table, HashingKey(req.family, req.key).asString)
    if (req.qualifiers == null) {
      indexedMap.foreach { case(key, (version, value)) =>
        kvs += new KeyValue(req.key, req.family, key.underlying, version, value)
      }
    } else {
      req.qualifiers().foreach { qualifier =>
        indexedMap.get(IndexingKey(qualifier)) match {
          case null =>
          case (version, value) =>
            kvs += new KeyValue(req.key, req.family, qualifier, version, value)
        }
      }
    }
    Deferred.fromResult(kvs)
  }

  def bufferAtomicIncrement(req: AtomicIncrementRequest): Deferred[java.lang.Long] = {
    logger.debug("bufferAtomicIncrement: " + req.toString)
    val hashKey = HashingKey(req.family, req.key).asString
    val indexingKey = IndexingKey(req.qualifier)
    var oldVal = 0L
    synchronized {
      val indexedMap = getTreeMap(req.table, hashKey)
      indexedMap.get(indexingKey) match {
        case null =>
          indexedMap.put(indexingKey, (0L, Bytes.fromLong(req.getAmount)))
        case (version, value) =>
          oldVal = Bytes.getLong(value)
          indexedMap.put(indexingKey, (version, Bytes.fromLong(oldVal + req.getAmount)))
      }
    }
    val long: java.lang.Long = oldVal
    Deferred.fromResult(long)
  }

  def compareAndSet(req: PutRequest, expected: Array[Byte]): Deferred[Boolean] = {
    logger.debug("compareAndSet: " + req.toString)
    val hashKey = HashingKey(req.family, req.key).asString
    val indexingKey = IndexingKey(req.qualifier)
    val ret = synchronized {
      val indexedMap = getTreeMap(req.table, hashKey)
      val check = indexedMap.get(indexingKey) match {
        case null => expected == null || expected.length == 0
        case (version, value) => Bytes.equals(value, expected)
      }

      if (check) {
        indexedMap.put(indexingKey, (req.timestamp, req.value))
        true
      } else {
        logger.error("check failed")
        false
      }
    }
    Deferred.fromResult(ret)
  }

  def flush(): Deferred[Boolean] = Deferred.fromResult(true)

  def put(req: PutRequest): Deferred[Boolean] = {
    logger.debug("put: " + req.toString)
    val hashKey = HashingKey(req.family, req.key).asString
    val indexingKey = IndexingKey(req.qualifier)
    val indexedMap = getTreeMap(req.table, hashKey)
    indexedMap.get(indexingKey) match {
      case null =>
        indexedMap.put(indexingKey, (req.timestamp, req.value))
      case (oldVersion, value) =>
        if (req.timestamp >= oldVersion) {
          indexedMap.put(indexingKey, (req.timestamp, req.value))
        }
    }
    Deferred.fromResult(true)
  }

  def delete(req: DeleteRequest): Deferred[Boolean] = {
    logger.debug("delete: " + req.toString)
    val hashKey = HashingKey(req.family, req.key).asString
    if (req.qualifiers == null) {
      deleteTreeMap(req.table, hashKey)
    } else {
      val indexedMap = getTreeMap(req.table, hashKey)
      req.qualifiers().foreach { qualifier =>
        indexedMap.remove(IndexingKey(qualifier))
      }
    }
    Deferred.fromResult(true)
  }
}

object MapDB {

  /** singleton */
  val client = new MapDBStorageClient

}
