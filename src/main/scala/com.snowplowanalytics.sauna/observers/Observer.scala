/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.sauna
package observers

// scala
import scala.util.control.NonFatal

// awscala
import awscala.sqs.SQS

// akka
import akka.actor.{Actor, ActorRef}

// java
import java.io.InputStream
import java.nio.file._

// sauna
import responders.Responder.S3Source
import loggers.Logger.Notification

/**
 * Observers are entities responsible for keeping eye on some source source of
 * events, like filesystem, AWS S3/SQS. Also it is on observer's
 * responsibility to manipulate and cleanup resources after event has been
 * processed.
 */
trait Observer {
  self: Actor =>
  /**
   * Send message to supervisor trait to forward it to loggers
   * This means observer should also be a child of supvervisor
   */
  def notify(message: String): Unit = {
    context.parent ! Notification(message)
  }
}

object Observer {

  /**
   * Common trait for observer events.
   */
  trait ObserverEvent extends Product with Serializable {
    /**
     * A full string representation of the event.
     */
    def id: String

    /**
     * The observer that emitted this event.
     */
    def observer: ActorRef
  }

  /**
   * Common trait for command-based events. Does not use an identifier and
   * always contains a content stream.
   */
  sealed trait ObserverCommandEvent extends ObserverEvent {
    /**
     * A stream containing the contents of the command.
     */
    def streamContent: InputStream
  }

  /**
   * Common trait for file-published events. Its' identifier is the path
   * to the file (so responders can decided whether handle it or not)
   * and it contains a content stream (so responders can read it)
   */
  sealed trait ObserverFileEvent extends ObserverEvent {
    /**
     * File's content stream. It can be streamed from local FS or from S3
     * None if file cannot be streamed
     * This should never be a part of object and wouldn't affect equality check
     */
    def streamContent: Option[InputStream]
  }

  /**
   * File has been published on local filesystem
   *
   * @param file full root of file
   */
  case class LocalFilePublished(file: Path, observer: ActorRef) extends ObserverFileEvent {

    def id = file.toAbsolutePath.toString

    def streamContent = try {
      Some(Files.newInputStream(file))
    } catch {
      case NonFatal(e) => None
    }
  }

  /**
   * File has been published on AWS S3
   *
   * @param id     full path on S3 bucket
   * @param s3Source AWS S3 credentials to access bucket and object
   */
  case class S3FilePublished(id: String, s3Source: S3Source, observer: ActorRef) extends ObserverFileEvent {
    def streamContent = s3Source.s3.get(s3Source.bucket, id).map(_.content)
  }

  /**
   * Messages signaling to observer that resources need to be cleaned-up
   */
  sealed trait DeleteFile extends Product with Serializable {
    private[observers] def run(): Unit
  }
  case class DeleteLocalFile(path: Path) extends DeleteFile {
    private[observers] def run(): Unit =
      Files.delete(path)
  }
  case class DeleteS3Object(path: String, s3Source: S3Source) extends DeleteFile {
    private[observers] def run(): Unit =
      s3Source.s3.deleteObject(s3Source.bucket.name, path)

    private[observers] def deleteMessage(sqs: SQS): Unit =
      sqs.delete(s3Source.message)
  }
}
