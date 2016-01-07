package com.kakao.s2graph.core.mysqls

import java.util.Properties

import com.kakao.s2graph.core.Management
import com.kakao.s2graph.core.Management.JsonModel.{Index, Prop}
import com.kakao.s2graph.core.mysqls.EtlParam.EtlType
import com.kakao.s2graph.core.types.HBaseType
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 7..
  */
class EtlSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val Ttl = 2

  val etlLabelName = "test_label_etl"

  val props = new Properties()
  props.setProperty("cache.ttl.seconds", Ttl.toString)
  val config = ConfigFactory.load(ConfigFactory.parseProperties(props))

  val cluster = config.getString("hbase.zookeeper.quorum")
  val serviceName = "_test_service"
  val serviceNameV2 = "_test_service_v2"
  val columnName = "user_id"
  val columnNameV2 = "user_id_v2"
  val columnType = "long"
  val columnTypeV2 = "long"
  val hTableName = "_test_cases"
  val preSplitSize = 0
  val labelName = "_test_label"

  val testProps = Seq(
    Prop("affinity_score", "0.0", "double"),
    Prop("is_blocked", "false", "boolean"),
    Prop("time", "0", "int"),
    Prop("weight", "0", "int"),
    Prop("is_hidden", "true", "boolean"),
    Prop("phone_number", "xxx-xxx-xxxx", "string"),
    Prop("score", "0.1", "float"),
    Prop("age", "10", "int")
  )
  val testIdxProps = Seq(Index("_PK", Seq("_timestamp", "affinity_score")))
  val hTableTTL = None

  override def beforeAll: Unit = {
    Model.apply(config)

    Management.createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL = None, "gz")
    Label.findByName(labelName, useCache = false) match {
      case Some(label) =>
        Etl.findByOriginalLabelIds(label.id.get, useCache = false).foreach { e =>
          Etl.delete(e.id)
        }
      case None =>
        Management.createLabel(labelName, serviceName, columnName, columnType, serviceName, columnName, columnType,
          isDirected = true, serviceName, testIdxProps, testProps, "weak", Some(hTableName), hTableTTL,
          HBaseType.VERSION3, isAsync = false)
    }
    if (Label.findByName(labelName, useCache = false).isEmpty) {
      Management.createLabel(labelName, serviceName, columnName, columnType, serviceName, columnName, columnType,
        isDirected = true, serviceName, testIdxProps, testProps, "weak", Some(hTableName), hTableTTL,
        HBaseType.VERSION3, isAsync = false)
    }
    if (Label.findByName(etlLabelName, useCache = false).isEmpty) {
      Management.createLabel(etlLabelName, serviceName, columnName, columnType, serviceName, columnName, columnType,
        isDirected = true, serviceName, testIdxProps, testProps, "weak", Some(hTableName), hTableTTL,
        HBaseType.VERSION3, isAsync = false)
    }
  }

  override def afterAll: Unit = {
    Label.findByName(labelName, useCache = false).foreach { label =>
      Etl.findByOriginalLabelIds(label.id.get, useCache = false).foreach { e =>
        Etl.delete(e.id)
      }
      Label.delete(label.id.get)
    }
    Label.findByName(etlLabelName, useCache = false).foreach { label =>
      Label.delete(label.id.get)
    }
  }

  "Etl" should "create one on DB" in {
    val originalLabel = Label.findByName(labelName)
    originalLabel should not be empty

    val etlLabel = Label.findByName(etlLabelName)
    etlLabel should not be empty

    val op = Etl.create(originalLabel.get.id.get, etlLabel.get.id.get, None, None, None)
    if (op.isFailure) {
      op.failed.map { ex => println(s"$ex")}
    }
    op.isSuccess should equal(true)
  }

  it should "find list by original label id" in {
    val originalLabel = Label.findByName(labelName)
    originalLabel should not be empty

    val etlLabel = Label.findByName(etlLabelName)
    etlLabel should not be empty

    val etls = Etl.findByOriginalLabelIds(originalLabel.get.id.get)
    etls should have size 1

    etls foreach { etl =>
      etl.originalLabelId should equal(originalLabel.get.id.get)
      etl.transformLabelId should equal(etlLabel.get.id.get)
    }
  }

  val logger = LoggerFactory.getLogger(getClass)

  "EtlParam" should "parse json" in {
    val s =
      """
        |{
        | "type": "BUCKET",
        | "value": "1"
        |}
      """.stripMargin
    val etlParam = Json.parse(s).validate[EtlParam]
    logger.info(s"${Json.parse(s)}, $etlParam")
    etlParam.isSuccess should equal(true)
    etlParam.get.`type` should equal(EtlType.BUCKET)
  }
}
