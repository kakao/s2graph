package org.apache.s2graph.lambda.source

import org.apache.spark.streaming.dstream.DStream

case class FileStreamSourceParams(interval: Long, path: String, timeout: Option[Long]) extends StreamingParams

class FileStreamSource(params: FileStreamSourceParams) extends StreamSource[String](params) {

  override protected lazy val stream: DStream[String] = streamingContext.textFileStream(params.path)
}

