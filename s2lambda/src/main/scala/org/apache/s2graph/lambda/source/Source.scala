package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda.{BaseDataProcessor, Data, EmptyData, Params}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.reflect.ClassTag

abstract class Source[O <: Data : ClassTag](params: Params) extends BaseDataProcessor[EmptyData, O](params)

trait RequiresSparkContext {
  private var _sparkContext: SparkContext = null

  def setSparkContext(sparkContext: SparkContext): Unit = _sparkContext = sparkContext

  lazy val sparkContext: SparkContext = {
    require(_sparkContext != null, "SparkContext never provided")
    _sparkContext
  }
}

trait RequiresSQLContext {
  var _sqlContext: SQLContext = null

  def setSQLContext(sqlContext: SQLContext): Unit = _sqlContext = sqlContext

  lazy val sqlContext: SQLContext = {
    require(_sqlContext != null, "SQLContext never provided")
    _sqlContext
  }
}


