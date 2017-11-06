/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.multitier

import retier.network._
import retier.util.Notifier

import akka.actor.{Actor, ActorRef}
import scala.collection.mutable
import scala.concurrent.Promise

class AkkaEnpoint(val establisher: ConnectionEstablisher, val actorRef: ActorRef)
    extends ProtocolInfo {
  def isEncrypted = false
  def isProtected = false
  def isAuthenticated = false
  def identification = None
}

case class AkkaMultitierMessage(data: Option[String])

class AkkaConnection(val protocol: AkkaEnpoint)(
  implicit val sender: ActorRef = Actor.noSender)
    extends Connection {
  private var open = true
  private val doClosed = Notifier[Unit]
  private val doReceive = Notifier[String]

  val closed = doClosed.notification
  val receive = doReceive.notification

  def isOpen = open

  def send(data: String) =
    protocol.actorRef ! AkkaMultitierMessage(Some(data))

  def close() = {
    protocol.actorRef ! AkkaMultitierMessage(None)
    open = false
    doClosed()
  }

  def process(message: AkkaMultitierMessage) = message.data match {
    case Some(data) =>
      doReceive(data)
    case None =>
      open = false
      doClosed()
  }
}

class AkkaConnectionRequestorFactory {
  val requestors = mutable.Map.empty[ActorRef, AkkaConnectionRequestor]

  def newConnection(actorRef: ActorRef)(
      implicit sender: ActorRef = Actor.noSender) = {
    val requestor = new AkkaConnectionRequestor
    requestor newConnection actorRef
    requestors += actorRef -> requestor
    requestor
  }

  def process(actorRef: ActorRef, message: AkkaMultitierMessage) =
    requestors get actorRef foreach { _ process message }
}

class AkkaConnectionRequestor extends ConnectionRequestor {
  private val promise = Promise[AkkaConnection]

  def request = promise.future

  def newConnection(actorRef: ActorRef)(
      implicit sender: ActorRef = Actor.noSender) =
    promise success new AkkaConnection(new AkkaEnpoint(this, actorRef))

  def process(message: AkkaMultitierMessage) =
    promise.future.value foreach { _ foreach { _ process (message) } }
}

class AkkaConnectionListener extends ConnectionListener {
  def start() = { }

  def stop() = { }

  val connections = mutable.Map.empty[ActorRef, AkkaConnection]

  def process(actorRef: ActorRef, message: AkkaMultitierMessage)(
      implicit sender: ActorRef = Actor.noSender) = {
    connections getOrElseUpdate (actorRef, {
      val connection = new AkkaConnection(new AkkaEnpoint(this, actorRef))
      connection.closed += { _ => connections -= actorRef }
      connections += actorRef -> connection
      doConnectionEstablished(connection)
      connection
    }) process message
  }
}
