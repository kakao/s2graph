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

package benchmark

import play.api.libs.json.JsNumber
import play.api.test.{FakeApplication, PlaySpecification, WithApplication}
import play.libs.Json

class JsonBenchmarkSpec extends BenchmarkCommon with PlaySpecification {
  "to json" should {
    implicit val app = FakeApplication()

    "json benchmark" in new WithApplication(app) {

      duration("map to json") {
        (0 to 100) foreach { n =>
          val numberMaps = (0 to 100).map { n => (n.toString -> JsNumber(n * n)) }.toMap
          Json.toJson(numberMaps)
        }
      }

      duration("directMakeJson") {
        (0 to 100) foreach { n =>
          var jsObj = play.api.libs.json.Json.obj()
          (0 to 100).foreach { n =>
            jsObj += (n.toString -> JsNumber(n * n))
          }
        }
      }

      duration("map to json 2") {
        (0 to 500) foreach { n =>
          val numberMaps = (0 to 100).map { n => (n.toString -> JsNumber(n * n)) }.toMap
          Json.toJson(numberMaps)
        }
      }

      duration("directMakeJson 2") {
        (0 to 500) foreach { n =>
          var jsObj = play.api.libs.json.Json.obj()
          (0 to 100).foreach { n =>
            jsObj += (n.toString -> JsNumber(n * n))
          }
        }
      }
    }
  }
}
