package s2.helper

import java.util.concurrent.Executors

import com.typesafe.config.Config
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager, HTableInterface}
import org.slf4j.LoggerFactory
import s2.config.S2CounterConfig

import scala.util.Try


class HBaseConnectionException(message: String, cause: Throwable=null) extends Exception(message, cause)
class HBaseStorageException(message: String, cause: Throwable=null) extends Exception(message, cause)

class WithHBase(config: Config) {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  lazy val s2config = new S2CounterConfig(config)

  lazy val zkQuorum = s2config.HBASE_ZOOKEEPER_QUORUM
  lazy val tableName = s2config.HBASE_TABLE_NAME
//  lazy val tablePool = Executors.newFixedThreadPool(s2config.HBASE_TABLE_POOL_SIZE)
//  lazy val connectionPool = Executors.newFixedThreadPool(s2config.HBASE_CONNECTION_POOL_SIZE)
//  lazy val ipcPool = s2config.HBASE_CLIENT_IPC_POOL_SIZE
  
  logger.info(s"Graph: $zkQuorum, $tableName")

  val hbaseConfig = HBaseConfiguration.create()
  s2config.getConfigMap("hbase").foreach { case (k, v) =>
    hbaseConfig.set(k, v)
  }

//  hbaseConfig.set("hbase.client.ipc.pool.size", s"$ipcPool")
//  hbaseConfig.set("hbase.zookeeper.quorum", zkQuorum)
//  hbaseConfig.set("hbase.client.max.total.tasks", s2config.HBASE_CLIENT_MAX_TOTAL_TASKS.toString)
//  hbaseConfig.set("hbase.client.max.perserver.tasks", s2config.HBASE_CLIENT_MAX_PERSERVER_TASKS.toString)
//  hbaseConfig.set("hbase.client.max.perregion.tasks", s2config.HBASE_CLIENT_MAX_PERREGION_TASKS.toString)
//  hbaseConfig.set("hbase.client.scanner.timeout.period", s2config.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD.toString)
//  hbaseConfig.set("hbase.client.operation.timeout", s2config.HBASE_CLIENT_OPERATION_TIMEOUT.toString)
//  hbaseConfig.set("hbase.client.retries.number", s2config.HBASE_CLIENT_RETRIES_NUMBER.toString)

  lazy val conn: HConnection = HConnectionManager.createConnection(hbaseConfig)

  val writeBufferSize = 1024 * 1024 * 2   // 2MB

  def apply[T](op: HTableInterface => T)(fallback: Throwable => T): T = {
    try {
      val table = conn.getTable(tableName)
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(writeBufferSize)
      try {
        op(table)
      } catch {
        case e: Throwable =>
          logger.error(s"Operation to table ($tableName) is failed: ${e.getMessage}")
          fallback(e)
//          throw new HBaseStorageException("[Error] HBase operation.", e)
      } finally {
        table.close()
      }
    } catch {
      case e: HBaseStorageException =>
        // fallback is already called
        throw e
      case e: Throwable =>
        logger.error(s"Getting table ($tableName) is failed: ${e.getMessage}")
        fallback(e)
//        throw new HBaseConnectionException("[Error] HBase get table failed.", e)
    }
  }
}
