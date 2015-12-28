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

package com.kakao.s2graph.core.storage

import com.kakao.s2graph.core._

import scala.collection.Seq
import scala.concurrent.ExecutionContext


abstract class MutationBuilder[T](storage: Storage)(implicit ex: ExecutionContext) {
  /** operation that needs to be supported by backend persistent storage system */
  def put(kvs: Seq[SKeyValue]): Seq[T]

  def increment(kvs: Seq[SKeyValue]): Seq[T]

  def delete(kvs: Seq[SKeyValue]): Seq[T]


  /** build mutation for backend persistent storage system */

  /** EdgeMutate */
  def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[T]

  def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[T]

  def increments(edgeMutate: EdgeMutate): Seq[T]

  /** IndexEdge */
  def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[T]

  def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[T]

  def buildDeletesAsync(indexedEdge: IndexEdge): Seq[T]

  def buildPutsAsync(indexedEdge: IndexEdge): Seq[T]

  /** SnapshotEdge */
  def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[T]

  def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[T]

  /** Vertex */
  def buildPutsAsync(vertex: Vertex): Seq[T]

  def buildDeleteAsync(vertex: Vertex): Seq[T]

  def buildDeleteBelongsToId(vertex: Vertex): Seq[T]

  def buildVertexPutsAsync(edge: Edge): Seq[T]

  def buildPutsAll(vertex: Vertex): Seq[T] = {
    vertex.op match {
      case d: Byte if d == GraphUtil.operations("delete") => buildDeleteAsync(vertex)
      case _ => buildPutsAsync(vertex)
    }
  }

}
