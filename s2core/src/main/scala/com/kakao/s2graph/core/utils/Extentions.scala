package com.kakao.s2graph.core.utils

import scala.concurrent.{ExecutionContext, Future}

object Extensions {

  implicit class FutureOps[T](f: Future[T]) {
    def retry(n: Int)(implicit ec: ExecutionContext): Future[T] = {
      n match {
        case i if i > 1 => f recoverWith { case t: Throwable => f retry (n - 1) }
        case _ =>
          logger.error(s"Future wait failed")
          f
      }
    }
  }

}
