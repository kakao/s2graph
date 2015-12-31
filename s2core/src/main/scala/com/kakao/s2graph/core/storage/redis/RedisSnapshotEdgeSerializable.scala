package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.SnapshotEdge
import com.kakao.s2graph.core.storage.{SKeyValue, StorageSerializable}

/**
  * Created by june.kay on 2015. 12. 31..
  */
class RedisSnapshotEdgeSerializable(snapshotEdge: SnapshotEdge) extends StorageSerializable[SnapshotEdge] {
  override def toKeyValues: Seq[SKeyValue] = ???
}
