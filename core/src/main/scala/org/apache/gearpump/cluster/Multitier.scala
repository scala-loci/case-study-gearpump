/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.cluster

import loci._
import loci.transmitter.rescala._

import rescala.default._

import akka.actor._
import scala.concurrent.Promise
import org.apache.gearpump.cluster.master.Master.MasterInfo
import org.apache.gearpump.cluster.worker.WorkerId
import org.apache.gearpump.multitier._

@multitier final class Multitier(implicit actorSystem: ExtendedActorSystem) {
  @peer type Master <: { type Tie <: Multiple[MasterProxy] with Multiple[Worker] }
  @peer type Worker <: { type Tie <: Single[MasterProxy] with Optional[Master] }
  @peer type MasterProxy <: { type Tie <: Multiple[Master] with Multiple[Worker] }

  val self: ActorRef

  val masterBirth: Local[Long] on Master
  def masterConnectWorker(worker: ActorRef): Local[AkkaConnector] on Master
  def masterRegisterNewWorker(): Local[WorkerId] on Master
  def masterRegisterWorker(workerId: WorkerId): Local[Unit] on Master

  def workerStarted(): Local[Unit] on Worker
  def workerRegistered(workerId: WorkerId, masterInfo: MasterInfo): Local[Unit] on Worker
  var masterConnected: Local[Boolean] on Worker = _

  def findMaster(): Local[Unit] on MasterProxy
  def masterConnected(master: Option[ActorRef]): Local[Unit] on MasterProxy

  on[MasterProxy] {
    findMaster()
    remote[Master].connected changedTo Seq.empty observe { _ =>
      findMaster()
    }
  }

  val registerNew = on[Worker] { Evt[ActorRef] }

  val registerNewWorker = on[MasterProxy] sbj { master: Remote[Master] =>
    (registerNew.asLocalFromAllSeq
      map { case (_, ref) => ref -> remote[Master].connected().headOption }
      collect { case (ref, connected) if connected contains master => ref })
  }

  on[Master] {
    registerNewWorker.asLocalFromAllSeq observe { case (_, ref) =>
      remote[Worker].connect(masterConnectWorker(ref))
    }

    remote[Worker].joined += { worker =>
      registerWorker(masterRegisterNewWorker(), worker)
    }
  }

  def main() = on[Worker] {
    registerNew.fire(self)
    workerStarted()
  }

  def registerWorker(workerId: WorkerId): Local[Unit] on Worker =
    on[Master].run.capture(workerId) sbj { worker: Remote[Worker] =>
      registerWorker(workerId, worker)
    }

  on[Worker] {
    remote[Master].connected observe { connected => masterConnected = connected.nonEmpty }
  }

  def workerConnectMaster(connector: AkkaConnector) = on[Worker] local {
    remote[Master].connect(connector)
  }

  def proxyConnectMaster(connector: AkkaConnector) = on[MasterProxy] local {
    remote[Master].connect(connector)
  }

  on[MasterProxy] {
    Signal { remote[Master].connected().headOption }.changed map {
      _ map { _.protocol.asInstanceOf[AkkaEnpoint].actorRef }
    } observe masterConnected
  }

  def registerWorker(workerId: WorkerId, worker: Remote[Worker]): Local[Unit] on Master = {
    masterRegisterWorker(workerId)
    val masterInfo = MasterInfo(self, masterBirth)
    on(worker).run.capture(workerId, masterInfo) {
      workerRegistered(workerId, masterInfo)
    }
  }
}
