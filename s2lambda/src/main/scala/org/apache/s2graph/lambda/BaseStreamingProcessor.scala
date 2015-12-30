package org.apache.s2graph.lambda

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time

abstract class BaseStreamingProcessor(params: Params)
    extends BaseDataProcessor[EmptyData, EmptyData](params) {

  def this() = this(Params.emptyOrNotGivenParams)

  def foreachRDD(foreachFunc: (RDD[_], Time) => Unit): Unit

}
