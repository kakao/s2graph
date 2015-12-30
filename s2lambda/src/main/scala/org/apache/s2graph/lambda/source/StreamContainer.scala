package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda.{BaseDataProcessor, Data, EmptyData, Params}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext, Time}

import scala.reflect.ClassTag

case class StreamFrontEndData[T: ClassTag](rdd: RDD[T], time: Time) extends Data

class StreamFrontEnd[T: ClassTag](params: Params, parent: String) extends BaseDataProcessor[EmptyData, StreamFrontEndData[T]](params) {

  var rdd: RDD[T] = null
  var time: Time = null

  override protected def processBlock(input: EmptyData): StreamFrontEndData[T] = {
    StreamFrontEndData(rdd, time)
  }

  def setRDD(rdd: RDD[T], time: Time): Unit = {
    this.rdd = rdd
    this.time = time
  }
}

trait StreamContainerParams extends Params {
  val interval: Long
  val timeout: Option[Long] // testing purpose
}

abstract class StreamContainer[T: ClassTag](params: StreamContainerParams)
    extends Source[EmptyData](params) with RequiresSparkContext {

  final def getParams: StreamContainerParams = params

  final val frontEnd: StreamFrontEnd[T] = new StreamFrontEnd[T](params, getClass.getSimpleName)

  final var _streamingContext: StreamingContext = null

  final override def setSparkContext(sc: SparkContext): Unit = {
    super.setSparkContext(sc)
    _streamingContext = new StreamingContext(sparkContext, Durations.seconds(params.interval))
  }

  final def streamingContext: StreamingContext = {
    require(_streamingContext != null, "StreamingContext never provided")
    _streamingContext
  }

  final def foreachBatch(foreachFunc: => Unit): Unit = {
    stream.foreachRDD { (rdd, time) =>
      frontEnd.setRDD(rdd, time)
      val rddAfterProcess = { foreachFunc; rdd }
      postProcess(rddAfterProcess, time)
    }
  }

  final override protected def processBlock(input: EmptyData): EmptyData = throw new IllegalAccessError

  protected val stream: DStream[T]

  def postProcess(rdd: RDD[T], time: Time): Unit = {}
}
