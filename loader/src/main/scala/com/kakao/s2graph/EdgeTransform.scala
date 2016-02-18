package com.kakao.s2graph

import com.kakao.s2graph.client._
import com.kakao.s2graph.core.mysqls.EtlParam.EtlType
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.{Edge, GraphUtil, Management}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
class EdgeTransform(rest: RestClient, readOnlyRest: RestClient) {
  val logger = LoggerFactory.getLogger(getClass)

  val experimentRest = RestClient()

  def transformEdge(edge: Edge)(implicit ec: ExecutionContext): Seq[Edge] = {
    transformEdges(Seq(edge))
  }

  def transformEdges(edges: Seq[Edge])(implicit ec: ExecutionContext): Seq[Edge] = {
    val future = Future.sequence[Try[Edge], Seq] {
      for {
        edge <- edges if edge.label.id.isDefined
        labelId = edge.label.id.get
        etl <- Etl.findByOriginalLabelIds(labelId)
        transformLabel <- Label.findByIdOpt(etl.transformLabelId)
        src = edge.srcVertex.innerId.toString() if src.nonEmpty
        tgt = edge.tgtVertex.innerId.toString() if tgt.nonEmpty
        propsWithName = edge.propsWithName
      } yield {
        // change src or target
        val srcFuture = evaluateEtlVertex(etl.srcEtlParam, src, propsWithName)
        val tgtFuture = evaluateEtlVertex(etl.tgtEtlParam, tgt, propsWithName)
        val propFuture = evaluateEtlProp(etl.propEtlParam, src, tgt, propsWithName)

        logger.info(s"$edge")

        for {
          newSrc <- srcFuture
          newTgt <- tgtFuture
          newProps <- propFuture
        } yield Try {
          Management.toEdge(
            edge.ts,
            GraphUtil.fromOp(edge.op),
            newSrc.get,
            newTgt.get,
            transformLabel.label,
            "out",
            Json.toJson(newProps).toString()
          )
        }
      }
    }.map(s => s.collect { case t: Success[Edge] => t.get } )

    Await.result(future, 10.seconds)
  }

  def loadEdges(edges: Seq[Edge], withWait: Boolean = false)(implicit ec: ExecutionContext): Seq[Boolean] = {
    edges match {
      case Nil => Nil
      case _ =>
        val payload = edges.map(_.toLogString).mkString("\n")
        val request = withWait match {
          case true =>
            BulkWithWaitRequest(payload)
          case false =>
            BulkRequest(payload)
        }
        val future = rest.post(request).map { resp =>
          logger.info(s"loadEdges: ${resp.json}")
          resp.json.as[Seq[Boolean]]
        }
        Await.result(future, 10.seconds)
    }
  }

  private def evaluateEtlVertex(etlParamOpt: Option[EtlParam],
                                original: String,
                                propsWithName: Map[String, JsValue])
                               (implicit ec: ExecutionContext): Future[JsResult[String]] = {
    etlParamOpt match {
      case Some(etlParam) =>
        etlParam.`type` match {
          case EtlType.QUERY =>
            runQuery(Json.obj()).map { js =>
              extractTargetVertex(js)
            }
          case EtlType.BUCKET =>
            runBucket(Json.obj(), etlParam.value.toInt, original).map { js =>
              extractTargetVertex(js)
            }
          case EtlType.EXPERIMENT =>
            runExperiment(Json.obj(), etlParam.value, original).map { js =>
              extractTargetVertex(js)
            }
          case EtlType.PROP =>
            Future.successful {
              propsWithName.get(etlParam.value).map(_.as[String]) match {
                case Some(s) => JsSuccess(s)
                case None => JsError(s"Doesn't exist a name ${etlParam.value}")
              }
            }
        }
      case None =>
        Future.successful(JsSuccess(original))
    }
  }

  private def evaluateEtlProp(etlParamOpt: Option[EtlParam], src: String, tgt: String, original: Map[String, JsValue])
                             (implicit ec: ExecutionContext): Future[Map[String, JsValue]] = {
    etlParamOpt match {
      case Some(etlParam) =>
        etlParam.`type` match {
          case EtlType.QUERY =>
            runQuery(Json.obj()).map { js =>
              extractProps(js).map { newProps =>
                original ++ newProps.as[JsObject].fields.toMap
              }.get
            }
          case EtlType.BUCKET =>
            val payload = Json.obj(
              "[[from]]" -> src,
              "[[to]]" -> tgt
            )
            runBucket(payload, etlParam.value.toInt, "").map { js =>
              extractProps(js).map { newProps =>
                original ++ newProps.as[JsObject].fields.toMap
              }.get
            }
          case EtlType.PROP =>
            Future.failed(new RuntimeException("Unsupported operation"))
        }
      case None =>
        Future.successful(original)
    }
  }

  private def runBucket(payload: JsValue, bucketId: Int, uuid: String)
                       (implicit ec: ExecutionContext): Future[JsValue] = {
    for {
      bucket <- Bucket.findById(bucketId)
      experiment <- Experiment.findById(bucket.experimentId)
      service <- Try { Service.findById(experiment.serviceId) }.toOption
    } yield ExperimentRequest(service.accessToken, experiment.name, uuid, payload)
  } match {
    case Some(req) => readOnlyRest.post(req).map(_.json)
    case None => Future.failed(new RuntimeException("cannot find experiment"))
  }

  private def runExperiment(payload: JsValue, url: String, uuid: String)
                           (implicit ec: ExecutionContext): Future[JsValue] = {
    val req = new RestRequest(s"$url/$uuid", payload)
    experimentRest.post(req).map(_.json)
  }

  private def runQuery(payload: JsValue): Future[JsValue] = ???

  private[s2graph] def extractTargetVertex(js: JsValue): JsResult[String] = {
    ((js \ "results")(0) \ "to").validate[JsValue].flatMap {
      case JsString(s) => JsSuccess(s)
      case JsNumber(n) => JsSuccess(n.toString())
      case _ => JsError("Unsupported vertex type")
    }
  }

  private[s2graph] def extractProps(js: JsValue): JsResult[JsObject] = {
    ((js \ "results")(0) \ "props").validate[JsObject]
  }
}
