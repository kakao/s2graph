package org.apache.s2graph.lambda.example

import org.apache.s2graph.lambda.{BaseDataProcessor, Data}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

case class WordCountData(counts: RDD[(String, Int)]) extends Data

class WordCount extends BaseDataProcessor[TextData, WordCountData] {

  override protected def processBlock(input: TextData): WordCountData = {
    val counts = input.rdd
        .flatMap(_.split("\\s+").filter(_.nonEmpty))
        .map(word => (word, 1))
        .reduceByKey(_ + _)

    WordCountData(counts)
  }

}
