package com.kakao.s2graph.client

import play.api.libs.json.JsValue

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 30..
  */
// for query
case class GetEdgesRequest(override val payload: JsValue) extends RestRequest("/graphs/getEdges", payload)
case class CheckEdgesRequest(override val payload: JsValue) extends RestRequest("/graphs/checkEdges", payload)
case class ExperimentRequest(accessToken: String, experimentName: String, uuid: String, override val payload: JsValue)
  extends RestRequest(s"/graphs/experiment/$accessToken/$experimentName/$uuid", payload)

// for mutation
case class InsertBulkRequest(override val payload: JsValue) extends RestRequest("/graphs/edges/insertBulk", payload)
case class InsertRequest(override val payload: JsValue) extends RestRequest("/graphs/edges/insert", payload)
case class InsertWithWaitRequest(override val payload: JsValue) extends RestRequest("/graphs/edges/insertWithWait", payload)
case class DeleteRequest(override val payload: JsValue) extends RestRequest("/graphs/edges/delete", payload)
case class BulkRequest(override val payload: String) extends RestRequest("/graphs/edges/bulk", payload)
case class BulkWithWaitRequest(override val payload: String) extends RestRequest("/graphs/edges/bulkWithWait", payload)
