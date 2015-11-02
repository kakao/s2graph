package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.storage.StorageSerializable

object HStorageSerializable {
  val vertexCf = "v".getBytes()
  val edgeCf = "e".getBytes()
}

trait HStorageSerializable extends StorageSerializable
