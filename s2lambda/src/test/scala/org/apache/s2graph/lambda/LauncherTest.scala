package org.apache.s2graph.lambda

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

class LauncherTest extends FunSuite with Matchers with LocalSparkContext {

//  val piJson =
//    """
//      |{
//      |  "name": "pi",
//      |  "comment": "Estimating Pi in https://spark.apache.org/examples.html",
//      |  "source": {
//      |    "class": "org.apache.s2graph.lambda.source.SQLContextSource"
//      |  },
//      |  "processors": [
//      |    {
//      |      "class": "org.apache.s2graph.lambda.example.Tuple2RandomNumberGenerator",
//      |      "params": {
//      |        "num": 1000
//      |      }
//      |    },
//      |    {
//      |      "class": "org.apache.s2graph.lambda.util.Inspector"
//      |    },
//      |    {
//      |      "class": "org.apache.s2graph.lambda.example.PiEstimator"
//      |    }
//      |  ]
//      |}
//      |
//    """.stripMargin
//
//  test("pi estimating") {
//    Launcher.launch(piJson, "run", Some(sc))
//  }
//
//  val wordCountJson =
//    """
//      |{
//      |  "name": "wordCount",
//      |  "comment": "Word Count in https://spark.apache.org/examples.html",
//      |  "source": {
//      |    "class": "org.apache.s2graph.lambda.source.SparkContextSource"
//      |  },
//      |  "processors": [
//      |    {
//      |      "class": "org.apache.s2graph.lambda.example.FileReader",
//      |      "params": {
//      |        "path": "LICENSE"
//      |      }
//      |    },
//      |    {
//      |      "class": "org.apache.s2graph.lambda.util.Inspector"
//      |    },
//      |    {
//      |      "class": "org.apache.s2graph.lambda.example.WordCount"
//      |    },
//      |    {
//      |      "class": "org.apache.s2graph.lambda.util.Inspector"
//      |    }
//      |  ]
//      |}
//      |
//    """.stripMargin
//
//  test("word count") {
//    Launcher.launch(wordCountJson, "run", Some(sc))
//  }

  val streamingWordCountJson =
    """
      |{
      |  "name": "streamingWordCount",
      |  "comment": "StreamingWordCount like NetworkWordCount"
      |  "source": {
      |    "class": "org.apache.s2graph.lambda.source.FileStreamSource",
      |    "params": {
      |      "interval": 1,
      |      "path": "aaa",
      |      "timeout": 60000
      |    }
      |  },
      |  "processors": [
      |    {
      |      "class": "org.apache.s2graph.lambda.example.WordCount",
      |    },
      |    {
      |      "class": "org.apache.s2graph.lambda.util.Inspector"
      |    }
      |  ]
      |}
      |
    """.stripMargin

  test("streaming") {
    Launcher.launch(streamingWordCountJson, "run", Some(sc))
  }

}
