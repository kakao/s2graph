package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.IndexEdge
import com.kakao.s2graph.core.storage.{SKeyValue, StorageSerializable}

/**
  * Created by june.kay on 2015. 12. 31..
  */
class RedisIndexEdgeSerializable(indexedEdge: IndexEdge) extends StorageSerializable[IndexEdge]{
  def toKeyValues: Seq[SKeyValue] = Nil
}
