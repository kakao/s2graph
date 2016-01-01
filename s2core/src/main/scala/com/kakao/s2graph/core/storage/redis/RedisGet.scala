package com.kakao.s2graph.core.storage.redis

/**
  * Unit to send get request to Redis
  * Created by june.kay on 2015. 12. 31..
  */
class RedisGet(key: Array[Byte]) {
  var timeout: Long = _
  var count: Int = _
  var offset: Int = _

  var min: Array[Byte] = _
  var minInclusive: Boolean = _
  var max: Array[Byte] = _
  var maxInclusive: Boolean = _

  var minTime: Long = _
  var maxTime: Long = _

  def setTimeout(time: Long): RedisGet = {
    this.timeout = time
    this
  }
  def setCount(count: Int): RedisGet = {
    this.count = count
    this
  }
  def setOffset(offset: Int): RedisGet = {
    this.offset = offset
    this
  }

  // for `interval`
  def setFilter(min: Array[Byte], minInclusive: Boolean, max: Array[Byte], maxInclusive: Boolean, minTime: Long = -1, maxTime: Long = -1): RedisGet = {
    this.min = min; this.minInclusive = minInclusive; this.max = max; this.maxInclusive = maxInclusive; this.minTime = minTime; this.maxTime = maxTime;
    this
  }

}
