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

import rescala._
import loci._
import loci.rescalaTransmitter._
import loci.util.Notification
import org.apache.gearpump.multitier._

import akka.actor._
import scala.concurrent.Promise
import org.apache.gearpump.cluster.master.Master.MasterInfo
import org.apache.gearpump.cluster.worker.WorkerId

@multitier
class Multitier()(implicit val actorSystem: ExtendedActorSystem) {
  trait Master extends Peer {
    type Tie <: Multiple[MasterProxy] with Multiple[Worker]
    val self: ActorRef
    val birth: Long
    def connectWorker(worker: ActorRef): AkkaConnectionRequestor
    def registerNewWorker(): WorkerId
    def registerWorker(workerId: WorkerId): Unit
  }

  trait Worker extends Peer {
    type Tie <: Single[MasterProxy] with Optional[Master]
    val self: ActorRef
    val registerWorker: Notification[WorkerId]
    val connectMaster: Notification[AkkaConnectionRequestor]
    def started(): Unit
    def masterConnected(connected: Boolean): Unit
    def workerRegistered(workerId: WorkerId, masterInfo: MasterInfo): Unit
  }

  trait MasterProxy extends Peer {
    type Tie <: Multiple[Master] with Multiple[Worker]
    val connectMaster: Notification[AkkaConnectionRequestor]
    def findMaster(): Unit
    def masterConnected(master: Option[ActorRef]): Unit
  }

  placed[MasterProxy] { implicit! =>
    peer.findMaster()
    remote[Master].connected changedTo Seq.empty observe { _ =>
      peer.findMaster()
    }
  }

  val registerNew = placed[Worker] { implicit! => Evt[ActorRef] }

  val registerNewWorker = placed[MasterProxy].sbj { implicit! => master: Remote[Master] =>
    registerNew.asLocalFromAllSeq filter { _ =>
      Some(master) == remote[Master].connected.now.headOption
    } map { case (_, ref) => ref }
  }

  placed[Master] { implicit! =>
    registerNewWorker.asLocalFromAllSeq observe { case (_, ref) =>
      remote[Worker] connect peer.connectWorker(ref)
    }

    remote[Worker].joined += { worker =>
      registerWorker(peer.registerNewWorker(), worker)
    }
  }

  placed[Worker].main { implicit! =>
    registerNew fire peer.self
    peer.started()
  }

  placed[Worker] { implicit! =>
    peer.registerWorker += { workerId =>
      remote[Master].sbj.capture(workerId) { implicit! => worker: Remote[Worker] =>
        registerWorker(workerId, worker)
      }
    }

    remote[Master].connected observe { connected => peer.masterConnected(connected.nonEmpty) }

    peer.connectMaster += { remote[Master] connect _ }
  }

  placed[MasterProxy] { implicit! =>
    peer.connectMaster += { remote[Master] connect _ }

    Signal { remote[Master].connected().headOption }.changed map {
      _ map { _.protocol.asInstanceOf[AkkaEnpoint].actorRef }
    } observe peer.masterConnected
  }

  def registerWorker(workerId: WorkerId, worker: Remote[Worker]) = placed[Master].local { implicit! =>
    peer.registerWorker(workerId)
    val masterInfo = MasterInfo(peer.self, peer.birth)
    remote.on(worker).capture(workerId, masterInfo) { implicit! =>
      peer.workerRegistered(workerId, masterInfo)
    }
  }
}
