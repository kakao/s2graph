package org.apache.s2graph.lambda

import java.util.UUID

import org.apache.s2graph.lambda.source.{HasSparkContext, StreamSource}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

case class ProcessorFactory(className: String, paramsAsJValue: JsonUtil.JsonValue) {

  private val constructor = Class.forName(className).getConstructors.head

  def getInstance: BaseDataProcessor[Data, Data] = {
    val instance = constructor.getParameterTypes match {
      case Array() =>
        constructor.newInstance()
      case Array(pClass) if classOf[Params].isAssignableFrom(pClass) =>
        val params = JsonUtil.extract(paramsAsJValue)(ClassTag(pClass)).asInstanceOf[Params]
        try {
          constructor.newInstance(params)
        } catch {
          case ex: java.lang.reflect.InvocationTargetException =>
            println(s"cause: ${ex.getCause}")
            throw ex
        }
      case _ =>
        require(false,
          Seq(constructor.getParameterTypes.toSeq, className, paramsAsJValue).map(_.toString).mkString(","))
    }
    instance.asInstanceOf[BaseDataProcessor[Data, Data]]
  }

}

case class StreamingProcessorDesc(
    `class`: String,
    params: Option[JsonUtil.JsonValue])

case class ProcessorDesc(
    `class`: String,
    params: Option[JsonUtil.JsonValue],
    id: Option[String],
    pid: Option[String],
    pids: Option[Seq[String]])

case class JobDesc(
    name: String,
    source: ProcessorDesc,
    processors: List[ProcessorDesc],
    root: Option[String],
    env: Option[Map[String, Any]],
    comment: Option[String])

object Launcher extends Visualization {

  val sparkUser = Option(sys.props("user.name")).getOrElse("unknown")
  val className: String = getClass.getName.stripSuffix("$")
  val name: String = className + "@" + sparkUser

  def main(args: Array[String]): Unit = {
    println(s"Running $name")

    require(args.length >= 2)

    val Array(command, encoded) = args.slice(0, 2)

    val jsonString = new String(javax.xml.bind.DatatypeConverter.parseBase64Binary(encoded), "UTF-8")

    launch(jsonString, command)
  }

  def buildPipeline(processors: List[ProcessorDesc], source: BaseDataProcessor[_, _] = null): List[BaseDataProcessor[Data, Data]] = {

    /** instantiation */
    val instances = processors.zipWithIndex.map { case (p, order) =>
      val id = p.id.getOrElse(UUID.randomUUID().toString)
      val pids = (p.pid, p.pids) match {
        case (None, None) => Seq.empty[String]
        case (Some(a), None) => Seq(a)
        case (None, Some(b)) => b
        case (Some(a), Some(b)) => {
          println("warn: Something wrong")
          Seq(a) ++ b
        }
      }
      val instance = ProcessorFactory(p.`class`, p.params.getOrElse(JsonUtil.emptyJsonValue)).getInstance
      (order, id, pids, instance)
    }

    /** add dependencies for each processor */
    val keyByOrder = instances.map(x => x._1 -> x).toMap
    val keyById = instances.map(x => x._2 -> x).toMap

    val rootId = "root"

    instances.foreach { case (order, id, pids, instance) =>
      val predecessors = pids match {
        case s if s.isEmpty && order == 0 && source == null => Seq.empty[BaseDataProcessor[_, _]]
        case s if s.isEmpty && order == 0 && source != null => Seq(source)
        case s if s.isEmpty => Seq(keyByOrder(order - 1)._4)
        case seq if seq.length == 1 && seq.contains(rootId) => Seq()
        case seq => seq.filter(_ != rootId).map(keyById).map(_._4)
      }

      val depth = predecessors.map(_.getDepth) match {
        case x if x.nonEmpty => x.max + 1
        case _ => 0
      }

      instance
          .setOrder(order)
          .setDepth(depth)
          .setPredecessors(predecessors: _*)
    }

    /** get dependency edges for visualization */
    val edges = instances.flatMap { case (order, id, pids, instance) =>
      if (instance.getPredecessors.isEmpty) {
        Seq("root" -> instance.toString(false))
      } else {
        instance.getPredecessors.map(_.toString(false) -> instance.toString(false))
      }
    }

    println("********** The Overall Execution Plan **********")
    println(getExecutionPlan(edges))

    println("********** Leaves **********")
    val leaves = instances.map(_._4).diff(instances.flatMap(_._4.getPredecessors))
    leaves.foreach(x => println(x.toString(false)))

    leaves
  }

  def launch(
      jsonString: String,
      command: String,
      givenSparkContext: Option[SparkContext] = None): Unit = {

    val jobDesc = JsonUtil.extract[JobDesc](jsonString)

    /** set env to JobContext */
    JobContext.setJobId(jobDesc.name)
    jobDesc.root.foreach(JobContext.setRootDir)
    jobDesc.comment.foreach(JobContext.setComment)

    /** show jobDesc */
    println(JsonUtil.toPrettyJsonString(jobDesc))

    val sparkContext = givenSparkContext.getOrElse {
      val sparkConf = new SparkConf()
          .setAppName(name + s"::${JobContext.jobId}::${JobContext.batchId}")
      new SparkContext(sparkConf)
    }

    val source =
      ProcessorFactory(jobDesc.source.`class`, jobDesc.source.params.getOrElse(JsonUtil.emptyJsonValue))
          .getInstance

    /** set SparkContext */
    val leaves = source match {
      case s if classOf[StreamSource[_]].isAssignableFrom(s.getClass) =>
        val streamSource = s.asInstanceOf[StreamSource[_]]
        streamSource.setSparkContext(sparkContext)
        val frontEnd = streamSource.frontEnd
        // streaming job
        buildPipeline(jobDesc.processors, frontEnd)
      case s: HasSparkContext =>
        s.setSparkContext(sparkContext)
        buildPipeline(jobDesc.processors, source)
      case _ =>
        buildPipeline(jobDesc.processors)
    }

    println(s"running ${JobContext.jobId}/${JobContext.batchId}: ${JobContext.comment}")

    if (command == "run") {
      source match {
        case s if classOf[StreamSource[_]].isAssignableFrom(s.getClass) =>
          val streamSource = s.asInstanceOf[StreamSource[_]]
          val ssc = streamSource.streamingContext

          streamSource.foreachBatch {
            leaves.foreach(_.process())
          }

          ssc.start()

          streamSource.getParams.timeout match {
            case Some(timeout) =>
              ssc.awaitTerminationOrTimeout(timeout)
            case None =>
              ssc.awaitTermination()
          }
        case _ =>
          leaves.foreach(_.process())
      }

    }
  }

}
