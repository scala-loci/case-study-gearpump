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

package org.apache.gearpump.cluster.master

import loci._
import org.apache.gearpump.multitier._
import org.apache.gearpump.cluster.Multitier

import scala.concurrent.duration.FiniteDuration

import akka.actor._
import org.slf4j.Logger

import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.{ActorUtil, LogUtil}

/**
 * This works with Master HA. When there are multiple Master nodes,
 * This will find a active one.
 */
class MasterProxy(masters: Iterable[ActorPath], timeout: FiniteDuration)
  extends Actor with Stash {
  import org.apache.gearpump.cluster.master.MasterProxy._

  val LOG: Logger = LogUtil.getLogger(getClass, name = self.path.name)

  val contacts = masters.map { url =>
    LOG.info(s"Contacts point URL: $url")
    context.actorSelection(url)
  }

  private val workerListener = new AkkaListener

  private val masterConnector = new AkkaConnectorFactory

  private val multitier = new Multitier()(context.system.asInstanceOf[ExtendedActorSystem])

  private var master = Option.empty[ActorRef]

  private def multitierInstance = multitierRuntime.instance.current.get

  private val multitierRuntime = loci.multitier start new Instance[multitier.MasterProxy](
      loci.contexts.Immediate.global,
      listen[multitier.Worker] { workerListener }) {

    implicit val self = MasterProxy.this.self

    def findMaster() =
      repeatActionUtil(timeout) {
        contacts foreach { contact =>
          LOG.info(s"sending identity to $contact")
          contact ! Identify(None)
        }
      }

    def masterConnected(master: Option[ActorRef]) = {
      MasterProxy.this.master = master
      if (master.nonEmpty) {
        watchers.foreach(_ ! MasterRestarted)
        unstashAll()
      }
    }
  }

  var watchers: List[ActorRef] = List.empty[ActorRef]

  import context.dispatcher

  LOG.info("Master Proxy is started...")

  override def postStop(): Unit = {
    watchers.foreach(_ ! MasterStopped)
    super.postStop()
  }

  override def receive: Receive = {
    case message: AkkaMultitierMessage if sender.path.name contains "master" =>
      masterConnector process (sender, message)
    case message: AkkaMultitierMessage if master.nonEmpty =>
      workerListener process (sender, message)
    case ActorIdentity(_, Some(receptionist)) =>
      multitierInstance retrieve multitier.proxyConnectMaster(masterConnector newConnection receptionist)
      if (master.isEmpty) {
        master = Some(receptionist)
      }
    case ActorIdentity(_, None) => // ok, use another instead
    case WatchMaster(watcher) =>
      watchers = watchers :+ watcher
    case msg => master match {
      case Some(master) =>
        LOG.debug(s"Get msg ${msg.getClass.getSimpleName}, forwarding to ${master.path}")
        master forward msg
      case _ =>
        stash()
    }
  }

//!  override def receive: Receive = {
//!    case message: AkkaMultitierMessage =>
//!      if (sender.toString contains "Worker")
//!        workerListener process (sender, message)
//!      else
//!        masterConnector process (sender, message)
//!    case _ =>
//!  }
//!
//!  def establishing(findMaster: Cancellable): Actor.Receive = {
//!    case ActorIdentity(_, Some(receptionist)) =>
//!      context watch receptionist
//!      LOG.info("Connected to [{}]", receptionist.path)
//!      context.watch(receptionist)
//!
//!      watchers.foreach(_ ! MasterRestarted)
//!      unstashAll()
//!      findMaster.cancel()
//!      context.become(active(receptionist) orElse messageHandler(receptionist))
//!    case ActorIdentity(_, None) => // ok, use another instead
//!    case msg =>
//!      LOG.info(s"Stashing ${msg.getClass.getSimpleName}")
//!      stash()
//!  }
//!
//!  def active(receptionist: ActorRef): Actor.Receive = {
//!    case Terminated(receptionist) =>
//!      LOG.info("Lost contact with [{}], reestablishing connection", receptionist)
//!      context.become(establishing(findMaster))
//!    case _: ActorIdentity => // ok, from previous establish, already handled
//!    case WatchMaster(watcher) =>
//!      watchers = watchers :+ watcher
//!  }
//!
//!  def messageHandler(master: ActorRef): Receive = {
//!    case msg =>
//!      LOG.debug(s"Get msg ${msg.getClass.getSimpleName}, forwarding to ${master.path}")
//!      master forward msg
//!  }

  def scheduler: Scheduler = context.system.scheduler
  import scala.concurrent.duration._
  private def repeatActionUtil(timeout: FiniteDuration)(action: => Unit): Cancellable = {
    val send = scheduler.schedule(0.seconds, 2.seconds)(action)
    val suicide = scheduler.scheduleOnce(timeout) {
      send.cancel()
      self ! PoisonPill
    }

    new Cancellable {
      def cancel(): Boolean = {
        val result1 = send.cancel()
        val result2 = suicide.cancel()
        result1 && result2
      }

      def isCancelled: Boolean = {
        send.isCancelled && suicide.isCancelled
      }
    }
  }
}

object MasterProxy {
  case object MasterRestarted
  case object MasterStopped
  case class WatchMaster(watcher: ActorRef)

  import scala.concurrent.duration._
  def props(masters: Iterable[HostPort], duration: FiniteDuration = 30.seconds): Props = {
    val contacts = masters.map(ActorUtil.getMasterActorPath(_))
    Props(new MasterProxy(contacts, duration))
  }
}