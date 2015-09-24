package s2.util

import java.io._
import java.net.{InetAddress, URI}
import java.util.concurrent.ConcurrentHashMap

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo
import org.apache.hadoop.ipc.StandbyException
import org.apache.http
import org.apache.http.entity.ContentType
import org.apache.spark.Logging

/**
 * Created by hsleep(honeysleep@gmail.com) on 14. 12. 31..
 */
object HdfsUtil extends Logging {
  val localHostname = InetAddress.getLocalHost.getHostName
  val contentType = ContentType.create("text/plain", http.Consts.UTF_8)
  val retryCnt = 5

  val nnByService = new ConcurrentHashMap[(String, String), String]

  val hadoopConfig: Configuration = new Configuration()
  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  hadoopConfig.set("fs.webhdfs.impl", classOf[org.apache.hadoop.hdfs.web.WebHdfsFileSystem].getName)

  def getActiveNode(zkQuorum: String, serviceId: String, refresh: Boolean = false): String = {
    val nnKey = (zkQuorum, serviceId)
    if (refresh || !nnByService.containsKey(nnKey)) {
      updateActiveNode(zkQuorum, serviceId)
    }
    nnByService.get(nnKey)
  }

  def updateActiveNode(zkQuorum: String, serviceId: String): Unit = {
    val nnKey = (zkQuorum, serviceId)
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val zkCli = CuratorFrameworkFactory.newClient(zkQuorum, retryPolicy)
    val zkPath = s"/hadoop-ha/$serviceId/ActiveStandbyElectorLock"

    zkCli.start()
    val data = zkCli.getData.forPath(zkPath)
    val nodeInfo = ActiveNodeInfo.parseFrom(data)
    zkCli.close()
    logInfo(s"updateActiveNode(${nodeInfo.getNameserviceId}): ${nnByService.get(nnKey)} -> ${nodeInfo.getHostname}")
    nnByService.put(nnKey, nodeInfo.getHostname)
  }

  def saveAsTextFile(uri: URI, body: Iterable[String]): Unit = {
    val fs = FileSystem.get(new URI(uri.getScheme, uri.getHost, null, null), hadoopConfig)

    val threadId = Thread.currentThread().getId
    val millis = System.currentTimeMillis()

    val fileName = s"part-$localHostname-$threadId-$millis"
    val tmpName = s".$fileName"

    val filePath = new Path(uri.getPath, fileName)
    val tmpPath = new Path(uri.getPath, tmpName)

    // temporary write 후에 성공하면 rename
    val outputStream = fs.create(tmpPath)
    val bufferedStream = new BufferedOutputStream(outputStream)
    body.foreach { line =>
      bufferedStream.write(line.getBytes())
      bufferedStream.write("\n".getBytes())
    }
    bufferedStream.close()
    outputStream.close()

    fs.rename(tmpPath, filePath)

    fs.close()
  }

  def withFS[T](uri: URI)(op: FileSystem => T)(fallback: Throwable => T): T = {
    var fs: FileSystem = null
    try {
      fs = FileSystem.get(new URI(uri.getScheme, uri.getHost, null, null), hadoopConfig)
      op(fs)
    } catch {
      case ex: Throwable =>
        fallback(ex)
    } finally {
      if (fs != null) {
        fs.close()
      }
    }
  }

  def withWriter[T](opener: Path => OutputStream, path: Path)(op: BufferedWriter => T)(fallback: Throwable => T): T = {
    var stream: OutputStream = null
    var writer: BufferedWriter = null
    try {
      stream = opener(path)
      writer = new BufferedWriter(new OutputStreamWriter(stream))
      op(writer)
    } catch {
      case ex: Throwable =>
        fallback(ex)
    } finally {
      if (writer != null) {
        writer.close()
      }
      if (stream != null) {
        stream.close()
      }
    }
  }

  def appendToTextFile(uri: URI, body: Iterable[String]): Unit = {
    withFS(new URI(uri.getScheme, uri.getHost, null, null)) { fs =>
      // set file path by hostname
      val threadId = Thread.currentThread().getId
      val fileName = s"part-$localHostname-$threadId"
      val filePath = new Path(uri.getPath, fileName)

      // make if not exist
      if (!fs.exists(filePath)) {
        fs.create(filePath).close()
      }

      // append data to file
      withWriter(fs.append, filePath) { writer =>
        body.foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      } { ex =>
        log.error(s"${ex.getMessage} (${filePath.toString})")
        throw ex
      }
    } { ex =>
      log.error(s"${ex.getMessage} (${uri.toString})")
      throw ex
    }
  }

  def operationHa(hdfsPath: String, zk: String, body: Iterable[String])(operation: (URI, Iterable[String]) => Unit): Unit = {
    /**
     * path example
     *  webhdfs://<host>/directory
     */
    val orgUri = new URI(hdfsPath)
    /**
     * zk example
     *  quorum1,quorum2/serviceId
     */
    lazy val Array(zkQuorum, serviceId) = zk.split('/')

    Retry(retryCnt) {
      try {
        if (zk == null) {
          operation(orgUri, body)
        }
        else {
          // active name node로 uri를 변경한다.
          val activeHost = getActiveNode(zkQuorum, serviceId)
          val newUri = new URI(orgUri.getScheme, activeHost, orgUri.getPath, null)
          operation(newUri, body)
        }
      } catch {
        case e: StandbyException if zk != null =>
          // active node를 갱신한다.
          updateActiveNode(zkQuorum, serviceId)
          throw e
        case e: IOException =>
          // logging and retry
          logWarning(e.getMessage)
          throw e
        case e: Throwable =>
          // 그 외 exception은 retry를 중단한다.
          throw new RetryStopException(e)
      }
    }
  }

  def saveAsTextFileHa(hdfsPath: String, zk: String, body: Iterable[String]): Unit = {
    /**
     * path example
     *  webhdfs://<host>/directory
     */
    val orgUri = new URI(hdfsPath)
    /**
     * zk example
     *  quorum1,quorum2/serviceId
     */
    lazy val Array(zkQuorum, serviceId) = zk.split('/')

    Retry(retryCnt) {
      try {
        if (zk == null) {
          saveAsTextFile(orgUri, body)
        }
        else {
          // active name node로 uri를 변경한다.
          val activeHost = getActiveNode(zkQuorum, serviceId)
          val newUri = new URI(orgUri.getScheme, activeHost, orgUri.getPath, null)
          saveAsTextFile(newUri, body)
        }
      } catch {
        case e: StandbyException if zk != null =>
          // active node를 갱신한다.
          updateActiveNode(zkQuorum, serviceId)
          throw e
        case e: IOException =>
          // logging and retry
          logWarning(e.getMessage)
          throw e
        case e: Throwable =>
          // 그 외 exception은 retry를 중단한다.
          throw new RetryStopException(e)
      }
    }
  }
}
