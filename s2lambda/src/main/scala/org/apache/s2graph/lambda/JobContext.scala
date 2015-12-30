package org.apache.s2graph.lambda

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext

object JobContext {

  private val BulkDirSuffix = "bulk"

  @transient private var _jobId: String = null
  
  @transient private var _rootDir: String = null

  @transient private var _comment = ""

  @transient private val _batchId: String = System.currentTimeMillis().toString

//  @transient private var _lastBatchId: String = null

  def setJobId(jobId: String): Unit =_jobId = jobId

  def setRootDir(rooDir: String): Unit = _rootDir = rooDir

  def setComment(comment: String): Unit = _comment = comment

  def jobId: String = {
    require(_jobId != null)
    _jobId
  }

  def rootDir: String = {
    require(_rootDir != null)
    _rootDir
  }

  def bulkDir: String = {
    rootDir + "/" + BulkDirSuffix
  }

//  def lastBatchId: String = {
//    if (_lastBatchId == null) {
//      try {
//        val fs = FileSystem.get(sparkContext.hadoopConfiguration)
//        val status = fs.listStatus(new Path(rootDir)).map(_.getPath.getName)
//        _lastBatchId =
//            status.filter(_ != BulkDirSuffix).filter(_ != batchId) match {
//              case empty if empty.isEmpty => batchId
//              case nonEmpty => nonEmpty.max
//            }
//      } catch {
//        case _: Throwable => _lastBatchId = batchId
//      }
//    }
//    _lastBatchId
//  }

  def batchId: String = _batchId

  def batchDir: String = s"$rootDir/$batchId"

//  def lastBatchDir: String = s"$rootDir/$lastBatchId"

  def comment: String = _comment

}
