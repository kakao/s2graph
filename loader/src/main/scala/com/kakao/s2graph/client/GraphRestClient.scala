package com.kakao.s2graph.client

import play.api.libs.json.JsValue
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ning.NingWSClient

import scala.concurrent.Future

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 30..
  */
class GraphRestRequest[T](val url: String, val payload: T)

// for query
case class GetEdgesRequest(override val payload: JsValue) extends GraphRestRequest("/graphs/getEdges", payload)
case class CheckEdgesRequest(override val payload: JsValue) extends GraphRestRequest("/graphs/checkEdges", payload)
case class ExperimentRequest(accessToken: String, experimentName: String, uuid: String, override val payload: JsValue)
  extends GraphRestRequest(s"/graphs/experiment/$accessToken/$experimentName/$uuid", payload)

// for mutation
case class InsertBulkRequest(override val payload: JsValue) extends GraphRestRequest("/graphs/edges/insertBulk", payload)
case class DeleteRequest(override val payload: JsValue) extends GraphRestRequest("/graphs/edges/delete", payload)
case class BulkRequest(override val payload: String) extends GraphRestRequest("/graphs/edges/bulk", payload)
case class BulkWithWaitRequest(override val payload: String) extends GraphRestRequest("/graphs/edges/bulkWithWait", payload)

class GraphRestClient(wsClient: NingWSClient, baseUrl: String) {
  def post(url: String, payload: JsValue): Future[WSResponse] = {
    wsClient.url(baseUrl + url).post(payload)
  }

  def post(url: String, payload: String): Future[WSResponse] = {
    wsClient.url(baseUrl + url).post(payload)
  }

  def post[T](req: GraphRestRequest[T]): Future[WSResponse] = {
    req.payload match {
      case js: JsValue => post(req.url, js)
      case str: String => post(req.url, str)
    }
  }
}
