package com.kakao.s2graph.core.storage.rocks

import org.rocksdb.{RocksDBException, RocksDB, Options}


class RocksDBStorage {
  RocksDB.loadLibrary()

  val options = new Options().setCreateIfMissing(true)
  var db: RocksDB = null
  try {
    // a factory method that returns a RocksDB instance
    db = RocksDB.open(options, "/tmp/rocks")

    // do something
  } catch {
    case e: RocksDBException =>

  }
}
