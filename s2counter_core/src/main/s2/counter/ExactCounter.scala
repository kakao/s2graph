package s2.counter

import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import s2.helper._
import s2.models.Counter
import s2.util.Hashes

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

@deprecated("use core.ExactCounter", "0.14")
class ExactCounter(config: Config) {
  private[counter] val withHBase = new WithHBase(config)

  private[counter] val cfs: Seq[(String, String)] = {
    // short term/ long term
    // minutely, hourly / daily, monthly, total
//    Seq(("s", "m"), ("s", "H"), ("l", "d"), ("l", "M"), ("l", "t"))
    Seq(("s", "H"), ("l", "d"), ("l", "M"), ("l", "t"))
  }

  private[counter] val blobCF = cfs.last._1.getBytes
  private[counter] val blobColumn = "b".getBytes

  private val log = LoggerFactory.getLogger(getClass)

  // Query 할 때, counter policy와 item key를 넣어주면 hbase에서 조회할 rowkey를 만들어준다.
  // topK 에서도 사용해야 한다.
  def buildExactKey(policy: Counter, item: Any): Array[Byte] = {
    buildExactKey(policy.id, policy.itemType match {
      case Counter.ItemType.BLOB => Hashes.sha1(item.asInstanceOf[String])
      case _ => item
    })
  }

  def buildExactKey(counterId: Int, item: Any): Array[Byte] = {
    val buff = new ArrayBuffer[Byte]
    // hash key
    val hash = Bytes.toBytes(Hashes.murmur3(item.toString)).take(Bytes.SIZEOF_SHORT)
    buff ++= hash
//    val hash = (murmur3(item.toString) >> 16).toShort
//    buff ++= Bytes.toBytes(hash)

    buff ++= Bytes.toBytes(counterId)
    item match {
      case i: Int =>
        buff ++= Bytes.toBytes(i)
      case l: Long =>
        buff ++= Bytes.toBytes(l)
      case s: String =>
        buff ++= s.getBytes
    }
    buff.toArray
  }

  def tsToMonthly(ts: Long): Long = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(ts)
    val newCal = Calendar.getInstance()
    newCal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), 1, 0, 0, 0)

    newCal.getTimeInMillis
  }

  def tsAmplifier(millis: Long): Map[String, Long] = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(millis)

    val newCal = Calendar.getInstance()
    newCal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), 1, 0, 0, 0)
    newCal.set(Calendar.MILLISECOND, 0)
    val month = newCal.getTimeInMillis
    Seq(Calendar.DATE).foreach(field => newCal.set(field, cal.get(field)))
    val day = newCal.getTimeInMillis
    Seq(Calendar.HOUR_OF_DAY).foreach(field => newCal.set(field, cal.get(field)))
    val hour = newCal.getTimeInMillis
    Seq(Calendar.MINUTE).foreach(field => newCal.set(field, cal.get(field)))
    val minute = newCal.getTimeInMillis

    for {
      (cf, column) <- cfs
    } yield {
      (column, column match {
        case "m" => minute
        case "H" => hour
        case "d" => day
        case "M" => month
        case "t" => 0L
      })
    }
  }.toMap

  def getInnerCount(policy: Counter, item: String, intervals: Array[String], from: Long, to: Long, dimensionMap: Map[String, Array[String]]): List[(String, String, Long, Long)] = {
    val watch = Stopwatch.createStarted()

    lazy val emptyRst = List.empty[(String, String, Long, Long)]
    lazy val messageForLog = s"${policy.service}.${policy.action}.$item ${intervals.toSeq}.$from.$to.$dimensionMap"

    val rowKey = buildExactKey(policy, item)
    log.debug(s"${rowKey.toSeq}")

    withHBase { table =>
      val gets = intervals.toList.map { interval =>
        val get = new Get(rowKey)
        Seq("m", "H").contains(interval) match {
          case true =>
            get.addFamily("s".getBytes)
          case false =>
            get.addFamily("l".getBytes)
        }
        get.setFilter(new ColumnRangeFilter(interval.getBytes ++ Bytes.toBytes(from), true, interval.getBytes ++ Bytes.toBytes(to), true))
      }

      val results = table.get(gets).toList
      Some(watch.elapsed(TimeUnit.MILLISECONDS)).filter(_ > 50).foreach { elapsed =>
        for {
          rst <- results
        } {
          if (!rst.isEmpty) {
            val mapped = rst.getMap.values()
            val mappedNo = rst.getNoVersionMap.values()

            val sum = mapped.flatMap { k =>
              k.map(innerMap => innerMap._2.keys.size)
            }.sum

            val sumNo = mappedNo.map(_.keys.size).sum

            log.warn(s"$elapsed ms: hbase get. $messageForLog [$sum ::: $sumNo]")
          }
        }
      }

      (for {
        rst <- results if !rst.isEmpty
      } yield {
        val rtn = {
          for {
            (familyBytes, qualifiers) <- rst.getNoVersionMap
            ((interval, dimension, ts), value) <- qualifiers.map(x => (ExactCounter.parseQualifier(x._1), Bytes.toLong(x._2)))
          } yield {
            (interval, dimension, ts, value)
          }
        }.toList
        Some(watch.elapsed(TimeUnit.MILLISECONDS)).filter(_ > 50).foreach { elapsed =>
          log.warn(s"$elapsed ms: hbase filter. $messageForLog")
        }
        rtn
      }).flatten
    } { ex =>
      emptyRst
    }
  }

  def getInnerCount3(policy: Counter, item: String, intervals: Array[String], limit: Int): List[(String, String, Long, Long)] = {
    val watch = Stopwatch.createStarted()

    lazy val emptyRst = List.empty[(String, String, Long, Long)]

    val rowKey = buildExactKey(policy, item)
    withHBase { table =>
      val get = new Get(rowKey)
      if (intervals.find(x => Seq("m", "H").contains(x)).isDefined) {
        // add short term family
        get.addFamily("s".getBytes)
      }
      if (intervals.find(x => Seq("d", "M", "t").contains(x)).isDefined) {
        // add long term family
        get.addFamily("l".getBytes)
      }
      val filters = for {
        interval <- intervals
      } yield {
        new ColumnPaginationFilter(limit, 0)
      }
      get.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, filters.toList))

      val rst = table.get(get)
      Some(watch.elapsed(TimeUnit.MILLISECONDS)).filter(_ > 50).foreach { elapsed =>
        if (!rst.isEmpty) {
          val mapped = rst.getMap.values()
          val mappedNo = rst.getNoVersionMap.values()

          val sum = mapped.flatMap{k =>
            k.map{innerMap  => innerMap._2.keys.size}
          }.foldLeft[Int](0)(_ + _)

          val sumNo = mappedNo.map(_.keys.size).foldLeft[Int](0)(_ + _)

          log.warn(s"$elapsed ms: hbase get. [$sum ::: $sumNo]")
        }
        else {
          log.warn(s"$elapsed ms: hbase get.")
        }
      }

      if (!rst.isEmpty) {
        val rtn = (for {
          (familyBytes, qualifiers) <- rst.getNoVersionMap
          ((interval, dimension, ts), value) <- qualifiers.map(x => (ExactCounter.parseQualifier(x._1), Bytes.toLong(x._2)))
        } yield {
          (interval, dimension, ts, value)
        }).toList
        Some(watch.elapsed(TimeUnit.MILLISECONDS)).filter(_ > 50).foreach { elapsed =>
          log.warn(s"$elapsed ms: hbase filter")
        }
        rtn
      }
      else {
        emptyRst
      }
    } { ex =>
      emptyRst
    }
  }

  def checkAndGetBlobValue(policy: Counter, blobId: String): String = {
    if (policy.itemType == Counter.ItemType.BLOB) {
      withHBase { table =>
        val rowKey = buildExactKey(policy.id, blobId)
        val rst = table.get(new Get(rowKey))
        if (!rst.isEmpty) {
          Bytes.toString(rst.getValue(blobCF, blobColumn))
        } else {
          blobId
        }
      } { ex =>
        blobId
      }
    } else {
      blobId
    }
  }
}

object ExactCounter {
  /**
   *
   * @param qualifier hbase column qualifier
   * @return interval, dimension, ts
   */
  def parseQualifier(qualifier: Array[Byte]): (String, String, Long) = {
    // qualifier: interval, ts, dimension 순서
    val interval = Bytes.toString(qualifier, 0, Bytes.SIZEOF_BYTE)
    val tsOffset = Bytes.SIZEOF_BYTE
    val ts = Bytes.toLong(qualifier, tsOffset)
    val dimensionOffset = tsOffset + Bytes.SIZEOF_LONG
    val dimension = Bytes.toString(qualifier, dimensionOffset)
    (interval, dimension, ts)
  }
}
