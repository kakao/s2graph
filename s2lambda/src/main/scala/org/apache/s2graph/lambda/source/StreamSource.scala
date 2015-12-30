package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda.{BaseDataProcessor, Data, EmptyData, Params}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext, Time}

import scala.reflect.ClassTag

trait StreamingParams extends Params {
  val interval: Long
  val timeout: Option[Long]
}

case class StreamSourceFrontEndData[T: ClassTag](rdd: RDD[T], time: Time) extends Data

class StreamSourceFrontEnd[T: ClassTag](params: Params, parent: String) extends BaseDataProcessor[EmptyData, StreamSourceFrontEndData[T]](params) {

  var rdd: RDD[T] = null
  var time: Time = null

  logInfo(s"parent: $parent")

  override protected def processBlock(input: EmptyData): StreamSourceFrontEndData[T] = {
    StreamSourceFrontEndData(rdd, time)
  }

  def setRDD(rdd: RDD[T], time: Time): Unit = {
    logInfo(s"==================> $time, ${rdd.take(10).mkString("\n")}")

    this.rdd = rdd
    this.time = time
  }
}

abstract class StreamSource[T: ClassTag](params: StreamingParams) extends BaseDataProcessor[EmptyData, EmptyData](params) with HasSparkContext {

  def getParams: StreamingParams = params

  protected val stream: DStream[T]

  val frontEnd: StreamSourceFrontEnd[T] = new StreamSourceFrontEnd[T](params, getClass.getSimpleName)

  var streamingContext: StreamingContext = null

  override def setSparkContext(sc: SparkContext): Unit =
    this.streamingContext = new StreamingContext(sc, Durations.seconds(params.interval))

  override protected def processBlock(input: EmptyData): EmptyData = Data.emptyData

  def foreachBatch(foreachFunc: => Unit): Unit = {
    stream.foreachRDD { (rdd, time) =>
      frontEnd.setRDD(rdd, time)
      logInfo("======== batch begin ========")
      foreachFunc
      logInfo("======== batch end ========")
    }
  }

}
