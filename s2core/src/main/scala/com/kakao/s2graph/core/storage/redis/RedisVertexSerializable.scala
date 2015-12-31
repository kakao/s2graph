package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.Vertex
import com.kakao.s2graph.core.storage.{SKeyValue, StorageSerializable}

/**
  * Created by june.kay on 2015. 12. 31..
  */
class RedisVertexSerializable(vertex: Vertex) extends StorageSerializable[Vertex] {
  override def toKeyValues: Seq[SKeyValue] = ???
}
