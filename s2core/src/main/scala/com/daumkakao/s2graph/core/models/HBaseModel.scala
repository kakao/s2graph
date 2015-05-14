package com.daumkakao.s2graph.core.models

import com.daumkakao.s2graph.core.HBaseElement.InnerVal
import com.daumkakao.s2graph.core.models.HBaseModel.{KEY, VAL}
import com.daumkakao.s2graph.core._
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, KeyValue, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import play.api.libs.json.{JsValue, JsObject, Json}
import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
object HBaseModel {
  val DELIMITER = ":"
  val KEY_VAL_DELIMITER = "^"
  val KEY_VAL_DELIMITER_WITH_ESCAPE = "\\^"
  val INNER_DELIMITER_WITH_ESCAPE = "\\|"
  val INNER_DELIMITER = "|"

  val modelTableName = "models"
  val modelCf = "m"
  val idQualifier = "i"
  val qualifier = "q"
  var zkQuorum: String = "localhost"

  type KEY = String
  type VAL = Any
  def newInstance(tableName: String)(kvs: Map[KEY, VAL]) = {
    tableName match {
      case "HService" => HService(kvs)
      case "HServiceColumn" => HServiceColumn(kvs)
      case "HColumnMeta" => HColumnMeta(kvs)
      case "HLabelMeta" => HLabelMeta(kvs)
      case "HLabelIndex" => HLabelIndex(kvs)
      case "HLabel" => HLabel(kvs)
      case _ => new HBaseModel(tableName, kvs)
    }
  }
  def apply(zkQuorum: String) = {
    this.zkQuorum = zkQuorum
  }
  def padZeros(v: VAL): String = {
    v match {
      case b: Byte => "%03d".format(b)
      case s: Short => "%05d".format(s)
      case i: Int => "%08d".format(i)
      case l: Long => "%08d".format(l)
      case ls: List[Any] => ls.mkString(",")
      case _ => v.toString
    }
  }

  def toKVTuplesMap(s: String) = {
    val tupleLs = for {
      kv <- s.split(KEY_VAL_DELIMITER_WITH_ESCAPE)
      t = kv.split(INNER_DELIMITER_WITH_ESCAPE) if t.length == 2
    } yield (t.head, t.last)
    tupleLs.toMap
  }
  def toKVs(kvs: Seq[(KEY, VAL)]) =  {
    val idxKVs = for {
      (k, v) <- kvs
    } yield s"$k$INNER_DELIMITER${padZeros(v)}"
    idxKVs.mkString(KEY_VAL_DELIMITER)
  }
  def toKVsWithFilter(kvs: Map[KEY, VAL],filterKeys: Seq[(KEY, VAL)]) = {
    val tgt = filterKeys.map(_._1).toSet
    val filtered = for {
      (k, v) <- kvs if !tgt.contains(k)
    } yield (k, padZeros(v))
    filtered.toSeq
  }
  def toRowKey(tableName: String, idxKeyVals: Seq[(KEY, VAL)]) = {
    List(tableName, toKVs(idxKeyVals)).mkString(DELIMITER)
  }

  def fromResult(r: Result): Option[HBaseModel] = {
    if (r == null | r.isEmpty) None
    else {
       r.listCells().headOption.map { cell =>
        val rowKey = Bytes.toString(cell.getRow)
        val qualifier = Bytes.toString(cell.getQualifier)
        val value = Bytes.toString(cell.getValue)
        val elements = rowKey.split(DELIMITER)
        val (tName, idxKeyVals) = (elements(0), elements(1))
        val merged = toKVTuplesMap(idxKeyVals) ++ toKVTuplesMap(value)
        newInstance(tName)(merged)
      }
    }
  }
  def fromResultLs(r: Result): List[HBaseModel] = {
    if (r == null | r.isEmpty) List.empty[HBaseModel]
    else {
      r.listCells().map { cell =>
        val rowKey = Bytes.toString(cell.getRow)
        val qualifier = Bytes.toString(cell.getQualifier)
        val value = Bytes.toString(cell.getValue)
        val elements = rowKey.split(DELIMITER)
        val (tName, idxKeyVals) = (elements(0), elements(1))
        val merged = toKVTuplesMap(idxKeyVals) ++ toKVTuplesMap(value)
        newInstance(tName)(merged)
      } toList
    }
  }

  def find(tableName: String)(idxKeyVals: Seq[(KEY, VAL)]): Option[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(tableName, idxKeyVals)
      val get = new Get(rowKey.getBytes)
      get.addColumn(modelCf.getBytes, qualifier.getBytes)
      get.setMaxVersions(1)
      val res = table.get(get)
      fromResult(res)
    } finally {
      table.close()
    }
  }
  def findsRange(tableName: String)(idxKeyVals: Seq[(KEY, VAL)], endIdxKeyVals: Seq[(KEY, VAL)]): List[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val scan = new Scan()
      scan.setStartRow(toRowKey(tableName, idxKeyVals).getBytes)
      scan.setStopRow(toRowKey(tableName, endIdxKeyVals).getBytes)
      scan.addColumn(modelCf.getBytes, qualifier.getBytes)
      val resScanner = table.getScanner(scan)
      val models = for {r <- resScanner; m <- fromResult(r)} yield m
      models.toList
    } finally {
      table.close()
    }
  }
  def findsMatch(tableName: String)(idxKeyVals: Seq[(KEY, VAL)]): List[HBaseModel] = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val scan = new Scan()
      scan.setStartRow(toRowKey(tableName, idxKeyVals).getBytes)
      val endBytes = Bytes.add(toRowKey(tableName, idxKeyVals).getBytes, Array.fill(1)(Byte.MinValue.toByte))
      scan.setStopRow(endBytes)
      scan.addColumn(modelCf.getBytes, qualifier.getBytes)
      val resScanner = table.getScanner(scan)
      val models = for {r <- resScanner; m <- fromResult(r)} yield m
      models.toList
    } finally {
      table.close()
    }
  }
  def getAndIncrSeq(tableName: String): Long = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      table.incrementColumnValue(tableName.getBytes, modelCf.getBytes, idQualifier.getBytes, 1L)
    } finally {
      table.close()
    }
  }

  def insert(tableName: String)(idxKVs: Seq[(KEY, VAL)], valKVs: Seq[(KEY, VAL)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      /** assumes using same hbase cluster **/
      val newSeq = getAndIncrSeq(tableName)
      val rowKey = toRowKey(tableName, idxKVs).getBytes
      val put = new Put(rowKey)
      put.addColumn(modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes)
      /** expecte null **/
      table.checkAndPut(rowKey, modelCf.getBytes, qualifier.getBytes, null, put)
    } finally {
      table.close()
    }
  }
  def delete(tableName: String)(idxKVs: Seq[(KEY, VAL)], valKVs: Seq[(KEY, VAL)]) = {
    val table = Graph.getConn(zkQuorum).getTable(TableName.valueOf(modelTableName))
    try {
      val rowKey = toRowKey(tableName, idxKVs).getBytes
      val delete = new Delete(rowKey)
      table.checkAndDelete(rowKey, modelCf.getBytes, qualifier.getBytes, toKVs(valKVs).getBytes, delete)
    } finally {
      table.close()
    }
  }
}

/**
 */
class HBaseModel(protected val tableName: String, protected val kvs: Map[KEY, VAL]) {
  import HBaseModel._
  protected val columns = Seq.empty[String]
  protected val idxKVsList = List.empty[Seq[(KEY, VAL)]]
  override def toString(): String = (kvs ++ Map("tableName" -> tableName)).toString

  def validate(columns: Seq[String]): Unit = {
    for (c <- columns) {
      if (!kvs.contains(c)) throw new RuntimeException(s"$tableName expect ${columns.toList.sorted}, found ${kvs.toList.sortBy{kv => kv._1}}")
    }
  }
  def create() = {
    val f = HBaseModel.insert(tableName)_
    val rets = for {
      idxKVs <- idxKVsList
    } yield {
      f(idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
  def destroy() = {
    val f = HBaseModel.delete(tableName)_
    val rets = for (idxKVs <- idxKVsList) yield {
      f(idxKVs, toKVsWithFilter(kvs, idxKVs))
    }
    rets.forall(r => r)
  }
}

object HColumnMeta {
  val timeStampSeq = 0.toByte
  val countSeq = -1.toByte
  val lastModifiedAtColumnSeq = 0.toByte
  val lastModifiedAtColumn = HColumnMeta(Map("id" -> 0, "columnId" -> 0,
    "name" -> "lastModifiedAt", "seq" -> lastModifiedAtColumnSeq))
  val maxValue = Byte.MaxValue


  def findById(id: Int): HColumnMeta = {
    HBaseModel.find("HColumnMeta")(Seq(("id" -> id))).get.asInstanceOf[HColumnMeta]
  }
  def findAllByColumn(columnId: Int) = {
    HBaseModel.findsMatch("HColumnMeta")(Seq(("columnId" -> columnId))).map(x => x.asInstanceOf[HColumnMeta])
  }
  def findByName(columnId: Int, name: String) = {
    HBaseModel.find("HColumnMeta")(Seq(("columnId" -> columnId), ("name" -> name))).map(x => x.asInstanceOf[HColumnMeta])
  }
  def findByIdAndSeq(columnId: Int, seq: Byte) = {
    HBaseModel.find("HColumnMeta")(Seq(("columnId" -> columnId), ("seq" -> seq))).map(x => x.asInstanceOf[HColumnMeta])
  }
  def findOrInsert(columnId: Int, name: String): HColumnMeta = {
    findByName(columnId, name) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HColumnMeta")
        val allMetas = findAllByColumn(columnId)
        val seq = (allMetas.length + 1).toByte
        val model = HColumnMeta(Map("id" -> id, "columnId" -> columnId, "name" -> name, "seq" -> seq))
        model.create
        model
    }
  }
}

case class HColumnMeta(kvsParam: Map[KEY, VAL]) extends HBaseModel("HColumnMeta", kvsParam) {
  override val columns = Seq("id", "columnId", "name", "seq")

  val pk = Seq(("id", kvs("id")))
  val idxColumnIdName = Seq(("columnId", kvs("columnId")), ("name", kvs("name")))
  val idxColumnIdSeq = Seq(("columnId", kvs("columnId")), ("seq", kvs("seq")))

  override val idxKVsList = List(pk, idxColumnIdName, idxColumnIdSeq)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val columnId = kvs("columnId").toString.toInt
  val name = kvs("name").toString
  val seq = kvs("seq").toString.toByte

}

object HService {
  def findById(id: Int): HService = {
    HBaseModel.find("HService")(Seq(("id" -> id))).get.asInstanceOf[HService]
  }
  def findByName(serviceName: String): Option[HService] = {
    HBaseModel.find("HService")(Seq(("serviceName" -> serviceName))).map { x => x.asInstanceOf[HService] }
  }
  def findOrInsert(serviceName: String, cluster: String, hTableName: String, preSplitSize: Int, hTableTTL: Option[Int]): HService = {
    findByName(serviceName) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HService")
        val kvs = Map("id" -> id, "serviceName" -> serviceName, "cluster" -> cluster, "hbaseTableName" -> hTableName,
          "preSplitSize" -> preSplitSize, "hbaseTableTTL" -> hTableTTL.getOrElse(-1))
        val service = HService(kvs)
        service.create()
        service
    }
  }
  def findAllServices(): List[HService] = {
    HBaseModel.findsRange("HService")(Seq(("id"-> 0)), Seq(("id" -> Int.MaxValue))).map{x => x.asInstanceOf[HService]}
  }
}
case class HService(kvsParam: Map[KEY, VAL]) extends HBaseModel("HService", kvsParam) {
  override val columns = Seq("id", "serviceName", "cluster", "hbaseTableName", "preSplitSize", "hbaseTableTTL")

  val pk = Seq(("id", kvs("id")))
  val idxServiceName = Seq(("serviceName", kvs("serviceName")))
  val idxCluster = Seq(("cluster", kvs("cluster")))

  override val idxKVsList = List(pk, idxServiceName, idxCluster)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val serviceName = kvs("serviceName").toString
  val cluster = kvs("cluster").toString
  val hTableName = kvs("hbaseTableName").toString
  val preSplitSize = kvs("preSplitSize").toString.toInt
  val hTableTTL = {
    val ttl = kvs("hbaseTableTTL").toString.toInt
    if (ttl < 0) None
    else Some(ttl)
  }
  lazy val toJson = kvs.toString
}

object HServiceColumn {
  def findById(id: Int): HServiceColumn = {
    HBaseModel.find("HServiceColumn")(Seq(("id" -> id))).get.asInstanceOf[HServiceColumn]
  }
  def find(serviceId: Int, columnName: String): Option[HServiceColumn] = {
    HBaseModel.find("HServiceColumn")(Seq("serviceId" -> serviceId, "columnName" -> columnName))
      .map { x => x.asInstanceOf[HServiceColumn]}
  }
  def findOrInsert(serviceId: Int, columnName: String, columnType: Option[String]): HServiceColumn = {
    find(serviceId, columnName) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HServiceColumn")
        val model = new HServiceColumn(Map("id" -> id, "serviceId" -> serviceId, "columnName" -> columnName,
        "columnType" -> columnType.getOrElse("string")))
        model.create
        model
    }
  }
}
case class HServiceColumn(kvsParam: Map[KEY, VAL]) extends HBaseModel("HServiceColumn", kvsParam) {
  override val columns = Seq("id", "serviceId", "columnName", "columnType")
  val pk = Seq(("id", kvs("id")))
  val idxServiceIdColumnName = Seq(("serviceId", kvs("serviceId")), ("columnName", kvs("columnName")))
  override val idxKVsList = List(pk, idxServiceIdColumnName)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val serviceId = kvs("serviceId").toString.toInt
  val columnName = kvs("columnName").toString
  val columnType = kvs("columnType").toString


  val service = HService.findById(serviceId)
  val metas = HColumnMeta.findAllByColumn(id.get)
  val metaNamesMap = (HColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq, x.name)) toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)
}
object HLabelMeta extends JSONParser {

  /** dummy sequences */
  val fromSeq = -4.toByte
  val toSeq = -5.toByte
  val lastOpSeq = -3.toByte
  val lastDeletedAt = -2.toByte
  val timeStampSeq = 0.toByte
  val countSeq = -1.toByte
  val maxValue = Byte.MaxValue
  val emptyValue = Byte.MaxValue

  /** reserved sequences */
  val from = HLabelMeta(Map("id" -> fromSeq, "labelId" -> fromSeq, "name" -> "_from", "seq" -> fromSeq,
    "defaultValue" -> fromSeq.toString,
    "dataType" -> "long", "usedInIndex" -> true))
  val to = HLabelMeta(Map("id" -> toSeq, "labelId" -> toSeq, "name" -> "_to", "seq" -> toSeq,
  "defaultValue" -> toSeq.toString, "dataType" -> "long", "usedInIndex" -> true))
  val timestamp = HLabelMeta(Map("id" -> -1, "labelId" -> -1, "name" -> "_timestamp", "seq" -> timeStampSeq,
  "defaultValue" -> "0", "dataType" -> "long", "usedInIndex" -> true))

  val reservedMetas = List(from, to, timestamp)
  val notExistSeqInDB = List(lastOpSeq, lastDeletedAt, countSeq, timeStampSeq, from.seq, to.seq)

  def findById(id: Int): HLabelMeta = {
    HBaseModel.find("HLabelMeta")(Seq(("id" -> id))).get.asInstanceOf[HLabelMeta]
  }
  def findAllByLabelId(labelId: Int): List[HLabelMeta] = {
    HBaseModel.findsMatch("HLabelMeta")(Seq(("labelId" -> labelId))).map { x => x.asInstanceOf[HLabelMeta] }
  }
  def findByName(labelId: Int, name: String): Option[HLabelMeta] = {
    name match {
      case timestamp.name => Some(timestamp)
      case to.name => Some(to)
      case _ =>
        HBaseModel.find("HLabelMeta")(Seq(("labelId" -> labelId), ("name" -> name))).map(x => x.asInstanceOf[HLabelMeta])
    }
  }
  def findOrInsert(labelId: Int, name: String, defaultValue: String, dataType: String, usedInIndex: Boolean): HLabelMeta = {
    findByName(labelId, name) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HLabelModel")
        val allMetas = findAllByLabelId(labelId)
        val seq = (allMetas.length + 1).toByte
        val model = HLabelMeta(Map("id" -> id, "labelId" -> labelId, "name" -> name, "seq" -> seq,
        "defaultValue" -> defaultValue, "dataType" -> dataType, "usedInIndex" -> usedInIndex))
        model.create
        model
    }
  }
  def convert(labelId: Int, jsValue: JsValue): Map[Byte, InnerVal] = {
    val ret = for {
      (k, v) <- jsValue.as[JsObject].fields
      meta <- HLabelMeta.findByName(labelId, k)
      innerVal <- jsValueToInnerVal(v, meta.dataType)
    } yield (meta.seq, innerVal)
    ret.toMap
  }
}
case class HLabelMeta(kvsParam: Map[KEY, VAL]) extends HBaseModel("HLabelMeta", kvsParam) with JSONParser {
  override val columns = Seq("id", "labelId", "name", "seq", "defaultValue", "dataType", "usedInIndex")
  val pk = Seq(("id", kvs("id")))
  val idxLabelIdName = Seq(("labelId", kvs("labelId")), ("name", kvs("name")))
  val idxLabelIdSeq = Seq(("labelId", kvs("labelId")), ("seq", kvs("seq")))
  override val idxKVsList = List(pk, idxLabelIdName, idxLabelIdSeq)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val labelId = kvs("labelId").toString.toInt
  val name = kvs("name").toString
  val seq = kvs("seq").toString.toByte
  val defaultValue = kvs("defaultValue").toString
  val dataType = kvs("dataType").toString
  val usedInIndex = kvs("usedInIndex").toString.toBoolean

  lazy val defaultInnerVal = if (defaultValue.isEmpty) InnerVal.withStr("") else toInnerVal(defaultValue, dataType)
  lazy val toJson = Json.obj("name" -> name, "defaultValue" -> defaultValue, "dataType" -> dataType, "usedInIndex" -> usedInIndex)
}
object HLabelIndex {
  val timestamp = HLabelIndex(Map("id" -> "0", "labelId" -> 0, "seq" -> 0.toByte, "metaSeqs" -> "0", "formular" -> ""))
  //  val withTsSeq = 0.toByte
  val defaultSeq = 1.toByte
  val maxOrderSeq = 7

  def findById(id: Int): HLabelIndex = {
    HBaseModel.find("HLabelIndex")(Seq(("id" -> id))).get.asInstanceOf[HLabelIndex]
  }
  def findByLabelIdAll(labelId: Int): List[HLabelIndex] = {
    HBaseModel.findsMatch("HLabelIndex")(Seq(("labelId" -> labelId))).map(x => x.asInstanceOf[HLabelIndex])
  }
  def findByLabelIdAndSeq(labelId: Int, seq: Byte): Option[HLabelIndex] = {
    HBaseModel.find("HLabelIndex")(Seq(("labelId" -> labelId), ("seq" -> seq))).map(x => x.asInstanceOf[HLabelIndex])
  }
  def findByLabelIdAndSeqs(labelId: Int, seqs: List[Byte]): Option[HLabelIndex] = {
    HBaseModel.find("HLabelIndex")(Seq(("labelId" -> labelId), ("metaSeqs" -> seqs.mkString(":"))))
      .map(x => x.asInstanceOf[HLabelIndex])
  }
  def findOrInsert(labelId: Int, seq: Byte, metaSeqs: List[Byte], formular: String): HLabelIndex = {
    findByLabelIdAndSeq(labelId, seq) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HLabelIndex")
        val model = HLabelIndex(Map("id" -> id, "labelId" -> labelId,
        "seq" -> seq, "metaSeqs" -> metaSeqs.mkString(":"), "formular" -> formular))
        model.create
        model
    }
  }
  def findOrInsert(labelId: Int, metaSeqs: List[Byte], formular: String): HLabelIndex = {
    findByLabelIdAndSeqs(labelId, metaSeqs) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq("HLabelIndex")
        val indices = HLabelIndex.findByLabelIdAll(labelId)
        val seq = (indices.length + 1).toByte
        val model = HLabelIndex(Map("id" -> id, "labelId" -> labelId,
          "seq" -> seq, "metaSeqs" -> metaSeqs.mkString(":"), "formular" -> formular))
        model.create
        model
    }
  }
}
case class HLabelIndex(kvsParam: Map[KEY, VAL]) extends HBaseModel("HLabelIndex", kvsParam) {
  override val columns = Seq("id", "labelId", "seq", "metaSeqs", "formular")
  val pk = Seq(("id", kvs("id")))
  val labelIdSeq = Seq(("labelId", kvs("labelId")), ("metaSeqs", kvs("metaSeqs")))
  override val idxKVsList = List(pk, labelIdSeq)
  println(s"$kvs")
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val labelId = kvs("labelId").toString.toInt
  val seq = kvs("seq").toString.toByte
  val metaSeqs = kvs("metaSeqs").toString.split(":").map(x => x.toByte).toList
  val formular = kvs("formular").toString

  lazy val label = HLabel.findById(labelId)
  lazy val metas = label.metaPropsMap
  lazy val sortKeyTypes = metaSeqs.map(metaSeq => label.metaPropsMap.get(metaSeq)).flatten
  lazy val sortKeyTypeDefaultVals = sortKeyTypes.map(x => x.defaultInnerVal)
  lazy val toJson = Json.obj("indexProps" -> sortKeyTypes.map(x => x.name))

}
object HLabel {
  val maxHBaseTableNames = 2
  type PROPS = (String, Any, String, Boolean)
  def findByName(labelUseCache: (String, Boolean)): Option[HLabel] = {
    findByName(labelUseCache._1)
  }
  def findByName(label: String): Option[HLabel] = {
    HBaseModel.find("HLabel")(Seq(("label" -> label))).map(x => x.asInstanceOf[HLabel])
  }
  def findById(id: Int): HLabel = {
    HBaseModel.find("HLabel")(Seq(("id" -> id))).get.asInstanceOf[HLabel]
  }
  def findByList(key: String, id: Int): List[HLabel] = {
    HBaseModel.findsMatch("HLabel")(Seq((key -> id))).map(x => x.asInstanceOf[HLabel])
  }
  def findByTgtColumnId(columnId: Int): List[HLabel] = {
    findByList("tgtColumnId", columnId)
  }
  def findBySrcColumnId(columnId: Int): List[HLabel] = {
    findByList("srcColumnId", columnId)
  }
  def findBySrcServiceId(serviceId: Int): List[HLabel] = {
    findByList("srcServiceId", serviceId)
  }
  def findByTgtServiceId(serviceId: Int): List[HLabel] = {
    findByList("tgtServiceId", serviceId)
  }
  def insertAll(labelName: String, srcServiceId: Int, srcColumnName: String, srcColumnType: String,
                 tgtServiceId: Int, tgtColumnName: String, tgtColumnType: String,
                 isDirected: Boolean = true, serviceName: String, serviceId: Int,
                 props: Seq[PROPS] = Seq.empty[PROPS],
                 consistencyLevel: String,
                 hTableName: Option[String],
                 hTableTTL: Option[Int]) = {
    val srcCol = HServiceColumn.findOrInsert(srcServiceId, srcColumnName, Some(srcColumnType))
    val tgtCol = HServiceColumn.findOrInsert(tgtServiceId, tgtColumnName, Some(tgtColumnType))
    val service = HService.findById(serviceId)
    //    require(service.id.get == srcServiceId || service.id.get == tgtServiceId)
    val createdId = HBaseModel.getAndIncrSeq("HLabel")
    val label = HLabel(Map("id" -> createdId,
      "label" -> labelName, "srcServiceId" -> srcServiceId,
    "srcColumnName" -> srcColumnName, "srcColumnType" -> srcColumnType,
    "tgtServiceId" -> tgtServiceId, "tgtColumnName" -> tgtColumnName, "tgtColumnType" -> tgtColumnType,
    "isDirected" -> isDirected, "serviceName" -> serviceName, "serviceId" -> serviceId,
    "consistencyLevel" -> consistencyLevel, "hTableName" -> hTableName.getOrElse("s2graph-dev"),
    "hTableTTL" -> hTableTTL.getOrElse(-1)))
    label.create

    val labelMetas =
      if (props.isEmpty) List(HLabelMeta.timestamp)
      else props.toList.map {
        case (name, defaultVal, dataType, usedInIndex) =>
          HLabelMeta.findOrInsert(createdId.toInt, name, defaultVal.toString, dataType, usedInIndex)
      }

    //    Logger.error(s"$labelMetas")
    val defaultIndexMetaSeqs = labelMetas.filter(_.usedInIndex).map(_.seq) match {
      case metaSeqs => if (metaSeqs.isEmpty) List(HLabelMeta.timestamp.seq) else metaSeqs
    }
    //    Logger.error(s"$defaultIndexMetaSeqs")
    //    kgraph.Logger.debug(s"Label: $defaultIndexMetaSeqs")
    /** deprecated */
    // 0 is reserved labelOrderSeq for delete, update
    //    LabelIndex.findOrInsert(createdId.toInt, 0, List(LabelMeta.timeStampSeq), "")
    HLabelIndex.findOrInsert(createdId.toInt, HLabelIndex.defaultSeq, defaultIndexMetaSeqs, "none")

    /** TODO: */
    (hTableName, hTableTTL) match {
      case (None, None) => // do nothing
      case (None, Some(hbaseTableTTL)) => throw new RuntimeException("if want to specify ttl, give hbaseTableName also")
      case (Some(hbaseTableName), None) =>
        // create own hbase table with default ttl on service level.
        Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, service.hTableTTL)
      case (Some(hbaseTableName), Some(hbaseTableTTL)) =>
        // create own hbase table with own ttl.
        Management.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, hTableTTL)
    }
  }
}
case class HLabel(kvsParam: Map[KEY, VAL]) extends HBaseModel("HLabel", kvsParam) with JSONParser {
  override val columns = Seq("id", "label", "srcServiceId", "srcColumnName", "srcColumnType",
  "tgtServiceId", "tgtColumnName", "tgtColumnType", "isDirected", "serviceName", "serviceId",
  "consistencyLevel", "hTableName", "hTableTTL")
  val pk = Seq(("id", kvs("id")))
  val idxLabel = Seq(("label", kvs("label")))
  val idxSrcColumnName = Seq(("srcColumnName", kvs("srcColumnName")))
  val idxTgtColumnName = Seq(("tgtColumnName", kvs("tgtColumnName")))
  val idxSrcServiceId = Seq(("srcServiceId", kvs("srcServiceId")))
  val idxtgtServiceId = Seq(("tgtServiceId", kvs("tgtServiceId")))
  val idxServiceName = Seq(("serviceName", kvs("serviceName")))
  val idxServiceId = Seq(("serviceId", kvs("serviceId")))

  override val idxKVsList = List(pk, idxLabel, idxSrcColumnName, idxTgtColumnName,
    idxSrcServiceId, idxtgtServiceId, idxServiceName, idxServiceId)
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val label = kvs("label").toString
  val srcServiceId = kvs("srcServiceId").toString.toInt
  val srcColumnName = kvs("srcColumnName").toString
  val srcColumnType = kvs("srcColumnType").toString

  val tgtServiceId = kvs("tgtServiceId").toString.toInt
  val tgtColumnName = kvs("tgtColumnName").toString
  val tgtColumnType = kvs("tgtColumnType").toString

  val isDirected = kvs("isDirected").toString.toBoolean
  val serviceName = kvs("serviceName").toString
  val serviceId = kvs("serviceId").toString.toInt

  val consistencyLevel = kvs("consistencyLevel").toString
  val hTableName = kvs("hTableName").toString
  val hTableTTL = {
    val ttl = kvs("hTableTTL").toString.toInt
    if (ttl < 0) None
    else Some(ttl)
  }


  def metas = HLabelMeta.findAllByLabelId(id.get)
  def metaSeqsToNames = metas.map(x => (x.seq, x.name)) toMap

  //  lazy val firstHBaseTableName = hbaseTableName.split(",").headOption.getOrElse(Config.HBASE_TABLE_NAME)
  lazy val service = HService.findById(serviceId)
  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, hTableName.split(",").head)
  lazy val (srcColumn, tgtColumn) = (HServiceColumn.find(srcServiceId, srcColumnName).get, HServiceColumn.find(tgtServiceId, tgtColumnName).get)
  lazy val direction = if (isDirected) "out" else "undirected"
  lazy val defaultIndex = HLabelIndex.findByLabelIdAndSeq(id.get, HLabelIndex.defaultSeq)

  //TODO: Make sure this is correct
  lazy val indices = HLabelIndex.findByLabelIdAll(id.get)
  lazy val indicesMap = indices.map(idx => (idx.seq, idx)) toMap
  lazy val indexSeqsMap = indices.map(idx => (idx.metaSeqs, idx)) toMap
  lazy val extraIndices = indices.filter(idx => defaultIndex.isDefined && idx.id.get != defaultIndex.get.id.get)
  //      indices filterNot (_.id.get == defaultIndex.get.id.get)
  lazy val extraIndicesMap = extraIndices.map(idx => (idx.seq, idx)) toMap

  lazy val metaProps = HLabelMeta.reservedMetas ::: HLabelMeta.findAllByLabelId(id.get)
  lazy val metaPropsMap = metaProps.map(x => (x.seq, x)).toMap
  lazy val metaPropsInvMap = metaProps.map(x => (x.name, x)).toMap
  lazy val metaPropNames = metaProps.map(x => x.name)
  lazy val metaPropNamesMap = metaProps.map(x => (x.seq, x.name)) toMap

  def init() = {
    metas
    metaSeqsToNames
    service
    srcColumn
    tgtColumn
    defaultIndex
    indices
    metaProps
  }
  def srcColumnInnerVal(jsValue: JsValue) = {
    jsValueToInnerVal(jsValue, srcColumnType)
  }
  def tgtColumnInnerVal(jsValue: JsValue) = {
    jsValueToInnerVal(jsValue, tgtColumnType)
  }

  override def toString(): String = {
    val orderByKeys = HLabelMeta.findAllByLabelId(id.get)
    super.toString() + orderByKeys.toString()
  }
  def findLabelIndexSeq(scoring: List[(Byte, Double)]): Byte = {
    if (scoring.isEmpty) HLabelIndex.defaultSeq
    else {
      HLabelIndex.findByLabelIdAndSeqs(id.get, scoring.map(_._1).sorted).map(_.seq).getOrElse(HLabelIndex.defaultSeq)
    }
  }

  lazy val toJson = Json.obj("labelName" -> label,
    "from" -> srcColumn.toJson, "to" -> tgtColumn.toJson,
    //    "indexProps" -> indexPropNames,
    "defaultIndex" -> defaultIndex.map(x => x.toJson),
    "extraIndex" -> extraIndices.map(exIdx => exIdx.toJson),
    "metaProps" -> metaProps.map(_.toJson) //    , "indices" -> indices.map(idx => idx.toJson)
  )

  def deleteAll() = {
    HLabelMeta.findAllByLabelId(id.get).foreach { x => HLabelMeta.findById(x.id.get).destroy() }
    //    LabelIndexProp.findAllByLabel(id.get, false).foreach { x => LabelIndexProp.delete(x.id.get) }
    HLabelIndex.findByLabelIdAll(id.get).foreach { x => HLabelIndex.findById(x.id.get).destroy() }
    HLabel.findById(id.get).destroy()
  }

}