/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package controllers

import com.kakao.s2graph.core.mysqls.Label
import play.api.http.HeaderNames
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

import scala.concurrent.Await

/**
 * Created by mojo22jojo(hyunsung.jo@gmail.com) on 15. 10. 13..
 */
class AdminControllerSpec extends SpecCommon {
  init()
  "EdgeControllerSpec" should {
    "update htable" in {
      running(FakeApplication()) {
        val insertUrl = s"/graphs/updateHTable/$testLabelName/$newHTableName"

        val req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")

        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Label.findByName(testLabelName, useCache = true).get.hTableName mustEqual newHTableName
      }
    }
  }
}
