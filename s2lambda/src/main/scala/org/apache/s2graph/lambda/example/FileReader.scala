package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda._
import org.apache.s2graph.lambda.source.SparkContextData
import org.apache.spark.rdd.RDD

case class FileReaderParams(path: String) extends Params

case class TextData(rdd: RDD[String]) extends Data

class FileReader(params: FileReaderParams)
    extends BaseDataProcessor[SparkContextData, TextData] {

  override protected def processBlock(input: SparkContextData): TextData =
    TextData(input.sparkContext.textFile(params.path))
}
