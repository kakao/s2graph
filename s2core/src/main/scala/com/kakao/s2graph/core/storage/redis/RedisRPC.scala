package com.kakao.s2graph.core.storage.redis

import org.apache.hadoop.hbase.util.Bytes

object RedisRPC {
  val DEGREE_EDGE_POSTFIX_BYTE = "-".getBytes
}


abstract class RedisRPC(key: Array[Byte])

case class RedisGetRequest(key: Array[Byte], isIncludeDegree: Boolean = true) extends RedisRPC(key){
  assert(key.length > 0)

  /**
    * For degree edge key(not sorted set key/value case)
    */
  lazy val degreeEdgeKey = Bytes.add(key, RedisRPC.DEGREE_EDGE_POSTFIX_BYTE)

  var timeout: Long = _
  var count: Int = _
  var offset: Int = _

  var min: Array[Byte] = _
  var minInclusive: Boolean = _
  var max: Array[Byte] = _
  var maxInclusive: Boolean = _

  var minTime: Long = _
  var maxTime: Long = _

  def setTimeout(time: Long): RedisGetRequest = {
    this.timeout = time
    this
  }
  def setCount(count: Int): RedisGetRequest = {
    this.count = count
    this
  }
  def setOffset(offset: Int): RedisGetRequest = {
    this.offset = offset
    this
  }

  // for `interval`
  def setFilter(min: Array[Byte],
    minInclusive: Boolean,
    max: Array[Byte],
    maxInclusive: Boolean,
    minTime: Long = -1,
    maxTime: Long = -1): RedisGetRequest = {

    this.min = min; this.minInclusive = minInclusive; this.max = max; this.maxInclusive = maxInclusive; this.minTime = minTime; this.maxTime = maxTime;
    this
  }

}

case class RedisPutRequest(key: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long) extends RedisRPC(key)

case class RedisAtomicIncrementRequest(key: Array[Byte], value: Array[Byte], delta: Long, isDegree: Boolean = false) extends RedisRPC(key) {
  /**
    * For degree edge key(not sorted set key/value case)
    */
  lazy val degreeEdgeKey = Bytes.add(key, RedisRPC.DEGREE_EDGE_POSTFIX_BYTE)
}

case class RedisDeleteRequest(key: Array[Byte], value: Array[Byte], timestamp: Long) extends RedisRPC(key)

