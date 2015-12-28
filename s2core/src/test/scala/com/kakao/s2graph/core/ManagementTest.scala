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

//package com.kakao.s2graph.core
//import com.kakao.s2graph.core.Graph
//import com.kakao.s2graph.core.models.{HLabel, HService, HServiceColumn, HBaseModel}
//import com.typesafe.config.ConfigFactory
//import org.scalatest.{FunSuite, Matchers}
//import play.api.libs.json.{JsString, JsBoolean, JsNumber, Json}
//
//import scala.concurrent.ExecutionContext
//
///**
// * Created by shon on 5/15/15.
// */
//class ManagementTest extends FunSuite with Matchers {
//  val labelName = "test_label"
//  val serviceName = "test_service"
//  val columnName = "test_column"
//  val columnType = "long"
//  val indexProps = Seq("weight" -> JsNumber(5), "is_hidden" -> JsBoolean(true))
//  val props = Seq("is_blocked" -> JsBoolean(true), "category" -> JsString("sports"))
//  val consistencyLevel = "weak"
//  val hTableName = Some("testHTable")
//  val hTableTTL = Some(86000)
//  val preSplitSize = 10
//  val zkQuorum = "localhost"
//
//  val config = ConfigFactory.parseString(s"hbase.zookeeper.quorum=$zkQuorum")
//  Graph(config)(ExecutionContext.Implicits.global)
//  HBaseModel(zkQuorum)
//  val current = System.currentTimeMillis()
//  val serviceNames = (0 until 2).map { i => s"$serviceName-${current + i}" }
//  val labelNames = (0 until 2).map { i => s"$labelName-${current + i}" }
//
////  def runTC[T <: HBaseModel](prevSeq: Long, prevSize: Int, prefix: String)(testSize: Int)(createF: String => T)(fetchF: String => Option[T])(deleteF: String => Boolean) = {
////    var lastSeq = prevSeq
////    val createds = collection.mutable.Map.empty[String, T]
////    val names = (0 until testSize) map { i => s"$prefix-${current + i}"}
////
////    val rets = for {
////      name <- names
////    } yield {
////        val created = createF(name)
////        val testSeq = created.id.get > lastSeq
////        lastSeq = created.id.get
////        createds += (name -> created)
////        val fetched = fetchF(name)
////        fetched.isDefined && created == fetched.get && testSeq
////      }
////
////    val deletes = for {
////      name <- names
////    } yield {
////        deleteF(name)
////      }
////
////    (rets ++ deletes).forall(_)
////  }
//  test("test create service") {
//
//    var prevSeq = Management.getSequence("HService")
//    val prevSize = HService.findAllServices().size
//    val createds = collection.mutable.Map.empty[String, HService]
//
//    val rets = for {
//      serviceName <- serviceNames
//    } yield {
//      val service = Management.createService(serviceName, zkQuorum, hTableName.get, preSplitSize, hTableTTL)
//      val testSeq = service.id.get > prevSeq
//      prevSeq = service.id.get
//      createds += (service.serviceName -> service)
//      val other = Management.findService(service.serviceName)
//      other.isDefined && service == other.get && testSeq
//    }
//
//    val deletes = for {
//      serviceName <- serviceNames
//    } yield {
//      Management.deleteService(serviceName)
//    }
//    (rets ++ deletes).forall(_)
//
//    HService.findAllServices().size == prevSize
//  }
//  test("test create label") {
//    val service = Management.createService(serviceName, zkQuorum, hTableName.get, preSplitSize, hTableTTL)
//    var prevSeq = Management.getSequence("HLabel")
//    val prevSize = HLabel.findAll(useCache = false)
//    val createds = collection.mutable.Map.empty[String, HLabel]
//
//    val rets = for {
//      lName <- labelNames
//    } yield {
//      val label = Management.createLabel(lName, serviceName, columnName, columnType,
//        serviceName, columnName, columnType,
//        true, serviceName, indexProps, props,
//        consistencyLevel, hTableName, hTableTTL
//      )
//      val testSeq = label.id.get > prevSeq
//      prevSeq = label.id.get
//
//      createds += (label.label -> label)
//      val other = Management.findLabel(label.label)
//      other.isDefined && label == other.get && testSeq
//    }
//    println(HLabel.findAll(useCache = false))
//    val deletes = for {
//      lName <- labelNames
//    } yield {
//        Management.deleteLabel(lName)
//      }
//    (rets ++ deletes).forall(_)
//    HLabel.findAll(useCache = false).size == prevSize
//  }
//  test("test update label") {
//    HLabel.updateLabel(labelName, Seq("is_blocked" -> JsBoolean(false)))
//    for {
//      label <- HLabel.findByName(labelName, useCache = false)
//    } yield {
//      println(label)
//    }
//  }
//}
