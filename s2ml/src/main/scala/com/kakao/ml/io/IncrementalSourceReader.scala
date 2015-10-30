package com.kakao.ml.io

import com.github.nscala_time.time.Imports._
import com.kakao.ml.{BaseDataProcessor, EmptyData, Params}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

case class IncrementalDataSourceParams(
    service: String,
    duration: Int,
    labelWeight: Map[String, Double],
    baseRoot: String,
    incRoot: String,
    baseBefore: Option[Int],
    exceptIncrementalData: Option[Boolean]) extends Params

class IncrementalDataSource(params: IncrementalDataSourceParams)
    extends BaseDataProcessor[EmptyData, SourceData](params) {

  val rawSchema = StructType(Seq(
    StructField("log_ts", LongType),
    StructField("operation", StringType),
    StructField("log_type", StringType),
    StructField("edge_from", StringType),
    StructField("edge_to", StringType),
    StructField("label", StringType),
    StructField("props", StringType),
    StructField("service", StringType),
    StructField("split", StringType),
    StructField("date_id", StringType)))

  def getIncrementalPaths(fs: FileSystem, split: String, lastDateId: String): String = {

    val dateIds = new ListBuffer[String]

    var dtVar = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(lastDateId)
    val base = DateTime.now()

    while(dtVar < base) {
      dateIds.append(dtVar.toString("yyyy-MM-dd"))
      dtVar = dtVar.plusDays(1)
    }

    val subDirs = dateIds
        .map { dateId => new Path(params.incRoot + s"/split=$split/date_id=$dateId") }
        .filter(fs.exists)
        .toArray

    val paths = fs.listStatus(subDirs).map(_.getPath.toString)

    if(paths.length < 2) {
      paths.foreach(p => logInfo(p))
    } else {
      logInfo(s"${paths.length} paths")
      logInfo(paths.head)
      logInfo("...")
      logInfo(paths.last)
    }

    paths.mkString(",")
  }

  override def processBlock(sqlContext: SQLContext, input: EmptyData): SourceData = {

    import sqlContext.implicits._

    if(predecessorData.asMap.contains("sourceDF")
        && predecessorData.asMap.contains("lastTS")) {

    }

    val sc = sqlContext.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val labelWeight = params.labelWeight
    val baseBefore = params.baseBefore.getOrElse(0)
    val split = if (fs.exists(new Path(params.baseRoot + s"/split=${params.service}"))) {
      params.service
    } else {
      "all"
    }

    sc.parallelize(Seq("dual")).toDF("dual")
        .select(lit("date between (a, b), inclusive") as "explain",
          date_sub(current_date(), params.duration + baseBefore) as "a",
          date_sub(current_date(), baseBefore) as "b")
        .show(false)

    /** from daily data */
    val dailyDF = sqlContext.read.schema(rawSchema).orc(params.baseRoot)
        .where($"split" === split
            and $"date_id".between(
          date_sub(current_date(), params.duration + baseBefore),
          date_sub(current_date(), baseBefore)))
        .where($"label".isin(labelWeight.keys.toSeq: _*))
        .select($"log_ts" as tsColString, $"label"
            as labelColString, $"edge_from" as userColString, $"edge_to" as itemColString)

    val exceptIncrementalData = if(baseBefore != 0) {
      true
    } else {
      params.exceptIncrementalData.getOrElse(false)
    }

    val sourceDF = if(exceptIncrementalData) {
      dailyDF
    } else {
      /** from incremental data */
      val lastDateId = fs.listStatus(new Path(s"${params.baseRoot}/split=$split"))
          .map(_.getPath.toString)
          .filter(_.contains("date_id=")) match {
        case x if x.nonEmpty => x.max.split("date_id=")(1)
        case _ => DateTime.now().minusDays(params.duration).toString("yyyy-MM-dd")
      }

      val bcLabelMap = sc.broadcast(labelWeight.keys.toSet)

      val incrementalDF = sc.textFile(getIncrementalPaths(fs, split, lastDateId)).map(_.split("\t", -1))
          .flatMap {
            case Array(log_ts, operation, log_type, edge_from, edge_to, l, props, s) if bcLabelMap.value.contains(l) =>
              Some((log_ts.toLong, l, edge_from, edge_to))
            case _ =>
              None
          }
          .toDF(tsColString, labelColString, userColString, itemColString)
      dailyDF.unionAll(incrementalDF)
    }

    /** materialize */
    sourceDF.persist(StorageLevel.MEMORY_AND_DISK)
    val (tsCount, tsFrom, tsTo) = sourceDF.select(count(tsCol), min(tsCol), max(tsCol)).head(1).map { row =>
      (row.getLong(0), row.getLong(1), row.getLong(2))
    }.head

    SourceData(sourceDF, tsCount, tsFrom, tsTo, labelWeight, None, None)
  }

}