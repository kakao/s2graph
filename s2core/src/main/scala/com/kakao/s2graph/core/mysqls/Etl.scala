package com.kakao.s2graph.core.mysqls

import play.api.libs.json._
import scalikejdbc._

import scala.util.Try

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 7..
  */
object Etl extends Model[Etl] {
  val e = Etl.syntax("e")
  val columnName = Etl.column

  def apply(c: SyntaxProvider[Etl])(rs: WrappedResultSet): Etl = apply(c.resultName)(rs)
  def apply(r: ResultName[Etl])(rs: WrappedResultSet): Etl = {
    Etl(rs.int(r.id), rs.int(r.originalLabelId), rs.int(r.transformLabelId),
      rs.stringOpt(r.srcEtl), rs.stringOpt(r.tgtEtl), rs.stringOpt(r.propEtl))
  }

  def create(originalLabelId: Int,
             transformLabelId: Int,
             srcEtl: Option[String] = None,
             tgtEtl: Option[String] = None,
             propEtl: Option[String] = None)
            (implicit session: DBSession = AutoSession): Try[Etl] = {
    Try {
      val id = withSQL {
        insert.into(Etl).namedValues(
          columnName.originalLabelId -> originalLabelId,
          columnName.transformLabelId -> transformLabelId,
          columnName.srcEtl -> srcEtl,
          columnName.tgtEtl -> tgtEtl,
          columnName.propEtl -> propEtl
        )
      }.updateAndReturnGeneratedKey().apply().toInt
      Etl(id, originalLabelId, transformLabelId, srcEtl, tgtEtl, propEtl)
    }
  }

  def findByOriginalLabelIds(originalLabelId: Int, useCache: Boolean = true)
                            (implicit session: DBSession = AutoSession): List[Etl] = {
    val cacheKey = s"originalLabelId=$originalLabelId"
    val sql = withSQL {
      selectFrom(Etl as e).where.eq(e.originalLabelId, originalLabelId)
    }.map(Etl(e))

    useCache match {
      case true =>
        withCaches(cacheKey) {
          sql.list().apply()
        }
      case false =>
        sql.list().apply()
    }
  }

  def delete(id: Int)
            (implicit session: DBSession = AutoSession): Try[Unit] = {
    Try {
      withSQL {
        deleteFrom(Etl).where.eq(columnName.id, id)
      }.update().apply()
    }
  }
}

object EtlParam {
  object EtlType extends Enumeration {
    val QUERY, PROP, BUCKET, EXPERIMENT = Value

    implicit val format = new Format[EtlType] {
      override def reads(json: JsValue): JsResult[EtlType] = {
        json.validate[String].map(s => EtlType.withName(s.toUpperCase))
      }

      override def writes(o: EtlType.Value): JsValue = {
        JsString(o.toString)
      }
    }
  }
  type EtlType = EtlType.Value

  implicit val format = Json.format[EtlParam]
}

case class EtlParam(`type`: EtlParam.EtlType, value: String)

case class Etl(id: Int,
               originalLabelId: Int,
               transformLabelId: Int,
               srcEtl: Option[String],
               tgtEtl: Option[String],
               propEtl: Option[String]) {
  import EtlParam._

  val srcEtlParam: Option[EtlParam] = srcEtl.flatMap(Json.parse(_).validate[EtlParam].asOpt)
  val tgtEtlParam: Option[EtlParam] = tgtEtl.flatMap(Json.parse(_).validate[EtlParam].asOpt)
  val propEtlParam: Option[EtlParam] = propEtl.flatMap(Json.parse(_).validate[EtlParam].asOpt)
}
