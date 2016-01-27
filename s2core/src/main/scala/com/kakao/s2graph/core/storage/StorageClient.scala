package com.kakao.s2graph.core.storage

import com.typesafe.config.Config

import scala.concurrent.Future


abstract class StorageClient[T](config: Config) {
  def delete(rpc: T): Future[Boolean]
  def put(rpc: T): Future[Boolean]
  def increment(rpc: T): Future[Long]
  def checkAndSet(rpc: T, expected: Array[Byte]): Future[Boolean]
}