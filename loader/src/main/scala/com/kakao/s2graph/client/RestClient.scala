package com.kakao.s2graph.client

import com.ning.http.client.AsyncHttpClientConfig
import org.slf4j.LoggerFactory
import play.api.libs.json.JsValue
import play.api.libs.ws.ning.NingWSClient
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.Future

/**
  * Created by hsleep(honeysleep@gmail.com) on 2016. 1. 28..
  */
class RestRequest[T](val url: String, val payload: T, val timeout: Option[Int] = None)

class RestClient(wSClient: WSClient, baseUrl: String) {
  def post(url: String, payload: JsValue, timeout: Option[Int]): Future[WSResponse] = {
    if (timeout.isDefined) {
      wSClient.url(baseUrl + url).withRequestTimeout(timeout.get).post(payload)
    } else {
      wSClient.url(baseUrl + url).post(payload)
    }
  }

  def post(url: String, payload: String, timeout: Option[Int]): Future[WSResponse] = {
    timeout match {
      case Some(to) =>
        wSClient.url(baseUrl + url).withRequestTimeout(to).post(payload)
      case None =>
        wSClient.url(baseUrl + url).post(payload)
    }
  }

  def post[T](req: RestRequest[T]): Future[WSResponse] = {
    req.payload match {
      case js: JsValue => post(req.url, js, req.timeout)
      case str: String => post(req.url, str, req.timeout)
    }
  }
}

object RestClient {
  val logger = LoggerFactory.getLogger(getClass)
  private lazy val builder = new AsyncHttpClientConfig.Builder()

  def apply(baseUrl: String = ""): RestClient = {
    val wSClient = new NingWSClient(builder.build)
    logger.info(s"wSClient.underlying: ${wSClient.underlying[com.ning.http.client.providers.netty.NettyAsyncHttpProvider]}")
    new RestClient(wSClient, baseUrl)
  }
}
