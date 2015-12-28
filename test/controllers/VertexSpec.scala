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

import play.api.libs.json._
import play.api.test.FakeApplication
import play.api.test.Helpers._

class VertexSpec extends SpecCommon {
  //  init()

  "vetex tc" should {
    "tc1" in {

      running(FakeApplication()) {
        val ids = (7 until 20).map(tcNum => tcNum * 1000 + 0)

        val (serviceName, columnName) = (testServiceName, testColumnName)

        val data = vertexInsertsPayload(serviceName, columnName, ids)
        val payload = Json.parse(Json.toJson(data).toString)
        println(payload)

        val jsResult = contentAsString(VertexController.tryMutates(payload, "insert",
          Option(serviceName), Option(columnName), withWait = true))

        val query = vertexQueryJson(serviceName, columnName, ids)
        val ret = contentAsJson(QueryController.getVerticesInner(query))
        println(">>>", ret)
        val fetched = ret.as[Seq[JsValue]]
        for {
          (d, f) <- data.zip(fetched)
        } yield {
          (d \ "id") must beEqualTo((f \ "id"))
          ((d \ "props") \ "age") must beEqualTo(((f \ "props") \ "age"))
        }
      }
      true
    }
  }
}
