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

import retier._
import org.apache.gearpump.multitier._
import retier.util.Notification

import akka.actor._
import scala.concurrent.Promise
import org.apache.gearpump.cluster.master.Master.MasterInfo
import org.apache.gearpump.cluster.worker.WorkerId

@multitier
class Multitier()(implicit val actorSystem: ExtendedActorSystem) {
  trait Master extends Peer {
    type Connection <: Multiple[Worker]
    val self: ActorRef
    val birth: Long
    def registerNewWorker(): WorkerId
    def registerWorker(workerId: WorkerId): Unit
  }

  trait Worker extends Peer {
    type Connection <: Single[Master]
    def workerRegistered(workerId: WorkerId, masterInfo: MasterInfo): Unit
    val registerNewWorker: Notification[Unit]
    val registerWorker: Notification[WorkerId]
    val connected = Promise[Unit]
  }

  placed[Worker] { implicit! =>
    peer.registerNewWorker += { _ =>
      remote[Master].issued { implicit! => worker: Remote[Worker] =>
        registerWorker(peer.registerNewWorker(), worker)
      }
    }

    peer.registerWorker += { workerId =>
      remote[Master].issued.capture(workerId) { implicit! => worker: Remote[Worker] =>
        registerWorker(workerId, worker)
      }
    }

    peer.connected success (())
  }

  def registerWorker(workerId: WorkerId, worker: Remote[Worker]) = placed[Master].local { implicit! =>
    peer.registerWorker(workerId)
    val masterInfo = MasterInfo(peer.self, peer.birth)
    remote.on(worker).capture(workerId, masterInfo) { implicit! =>
      peer.workerRegistered(workerId, masterInfo)
    }
  }
}
