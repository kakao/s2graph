package org.apache.s2graph.lambda.source

import org.apache.s2graph.lambda.{BaseDataProcessor, Data, EmptyData}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

case class SparkContextData(sparkContext: SparkContext) extends Data

case class SQLContextData(sqlContext: SQLContext) extends Data

trait HasSparkContext {
  def setSparkContext(sc: SparkContext): Unit
}

class SparkContextSource extends BaseDataProcessor[EmptyData, SparkContextData] with HasSparkContext {

  var sparkContext: SparkContext = null

  override protected def processBlock(input: EmptyData): SparkContextData = {
    require(sparkContext != null, "sparkContext never provided")
    SparkContextData(sparkContext)
  }

  override def setSparkContext(sc: SparkContext): Unit = this.sparkContext = sc
}

class SQLContextSource extends BaseDataProcessor[EmptyData, SQLContextData] with HasSparkContext {

  var sqlContext: SQLContext = null

  override protected def processBlock(input: EmptyData): SQLContextData = {
    require(sqlContext != null, "sqlContext never provided")
    SQLContextData(sqlContext)
  }

  override def setSparkContext(sc: SparkContext): Unit = this.sqlContext = new HiveContext(sc)
}
