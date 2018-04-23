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

package org.apache.gearpump

import loci._
import loci.transmission.{PullBasedTransmittable, RemoteRef, Serializable}

import akka.actor.ExtendedActorSystem
import akka.serialization.JavaSerializer
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64
import scala.reflect.ClassTag
import scala.util.Try

package object multitier {
  implicit def transmittableAny = new PullBasedTransmittable[Any, Any, Any] {
    def send(value: Any, remote: RemoteRef) = value
    def receive(value: Any, remote: RemoteRef) = value
  }

  implicit def serializableAny[T: ClassTag](implicit actorSystem: ExtendedActorSystem) =
    new Serializable[T] {
      def serialize(value: T) = JavaSerializer.currentSystem.withValue(actorSystem) {
        val arrayStream = new ByteArrayOutputStream
        val objectStream = new ObjectOutputStream(arrayStream)
        objectStream writeObject value
        Base64.getEncoder encodeToString arrayStream.toByteArray
      }

      def deserialize(value: String) = Try {
        JavaSerializer.currentSystem.withValue(actorSystem) {
          (implicitly[ClassTag[T]].runtimeClass cast
            new ObjectInputStream(
              new ByteArrayInputStream(
                Base64.getDecoder decode value.getBytes)).readObject).asInstanceOf[T]
        }
      }
    }
}
