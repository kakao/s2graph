package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda._
import org.apache.s2graph.lambda.source.SQLContextData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class Tuple2RandomNumberParams(num: Int) extends Params

case class Tuple2RandomNumberData(rdd: RDD[(Double, Double)], df: DataFrame, num: Int) extends Data

class Tuple2RandomNumberGenerator(params: Tuple2RandomNumberParams)
    extends BaseDataProcessor[SQLContextData, Tuple2RandomNumberData](params) {

  override protected def processBlock(input: SQLContextData): Tuple2RandomNumberData = {

    import input.sqlContext.implicits._

    val rdd = input.sqlContext.sparkContext.parallelize(1 to params.num).map{i =>
      (Math.random(), Math.random())
    }

    val df = rdd.toDF("x", "y")

    Tuple2RandomNumberData(rdd, df, params.num)
  }
}
