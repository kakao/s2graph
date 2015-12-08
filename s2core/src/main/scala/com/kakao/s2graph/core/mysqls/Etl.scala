package com.kakao.s2graph.core.mysqls

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
    Etl(Option(rs.int(r.id)), rs.int(r.originalLabelId), rs.int(r.transformLabelId),
      rs.intOpt(r.srcEtlQueryId), rs.intOpt(r.tgtEtlQueryId), rs.intOpt(r.propEtlQueryId))
  }

  def create(originalLabelId: Int,
             transformLabelId: Int,
             srcEtlQueryId: Option[Int] = None,
             tgtEtlQueryId: Option[Int] = None,
             propEtlQueryId: Option[Int] = None)
            (implicit session: DBSession = AutoSession): Try[Etl] = {
    Try {
      val id = withSQL {
        insert.into(Etl).namedValues(
          columnName.originalLabelId -> originalLabelId,
          columnName.transformLabelId -> transformLabelId,
          columnName.srcEtlQueryId -> srcEtlQueryId,
          columnName.tgtEtlQueryId -> tgtEtlQueryId,
          columnName.propEtlQueryId -> propEtlQueryId
        )
      }.updateAndReturnGeneratedKey().apply().toInt
      Etl(Some(id), originalLabelId, transformLabelId, srcEtlQueryId, tgtEtlQueryId, propEtlQueryId)
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

case class Etl(id: Option[Int],
               originalLabelId: Int,
               transformLabelId: Int,
               srcEtlQueryId: Option[Int],
               tgtEtlQueryId: Option[Int],
               propEtlQueryId: Option[Int])
