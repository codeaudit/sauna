/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package responders
package hipchat

// java
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

// iglu
import com.snowplowanalytics.iglu.core.Containers.SelfDescribingData
import com.snowplowanalytics.iglu.core.SchemaKey

// play
import play.api.libs.functional.syntax._
import play.api.libs.json._

// akka
import akka.actor.{ActorRef, Props}

// sauna
import Responder._
import SendRoomNotificationResponder._
import apis.Hipchat
import apis.Hipchat._
import loggers.Logger.Notification
import observers.Observer.{ ObserverEvent, ObserverCommandEvent }

class SendRoomNotificationResponder(hipchat: Hipchat, val logger: ActorRef) extends Responder[ObserverCommandEvent, RoomNotificationReceived] {

  override def extractEvent(observerEvent: ObserverEvent): Option[RoomNotificationReceived] = {
    observerEvent match {
      case e: ObserverCommandEvent =>
        val commandJson = Json.parse(Source.fromInputStream(e.streamContent).mkString)
        extractCommand[RoomNotification](commandJson) match {
          case Right((envelope, data)) =>
            processEnvelope(envelope) match {
              case None =>
                Some(RoomNotificationReceived(data, e))
              case Some(error) =>
                logger ! Notification(error)
                None
            }
          case Left(error) =>
            logger ! Notification(error)
            None
        }
      case _ => None
    }
  }

  /**
   * Send a valid room notification using the API wrapper.
   *
   * @param event The event containing a room notification.
   */
  override def process(event: RoomNotificationReceived): Unit =
    hipchat.sendRoomNotification(event.data).onComplete {
      case Success(message) => context.parent ! RoomNotificationSent(event, s"Successfully sent HipChat notification: $message")
      case Failure(error) => logger ! Notification(s"Error while sending HipChat notification: $error")
    }
}

object SendRoomNotificationResponder {
  case class RoomNotificationReceived(
    data: RoomNotification,
    source: ObserverCommandEvent
  ) extends ResponderEvent

  /**
   * A responder result denoting that a HipChat room notification was successfully sent
   * by the responder.
   *
   * @param source  The responder event that triggered this.
   * @param message A success message.
   */
  case class RoomNotificationSent(
    source: RoomNotificationReceived,
    message: String) extends ResponderResult[ObserverCommandEvent]

  /**
   * Constructs a [[Props]] for a [[SendRoomNotificationResponder]] actor.
   *
   * @param hipchat The HipChat API wrapper.
   * @param logger  A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(hipchat: Hipchat, logger: ActorRef): Props =
    Props(new SendRoomNotificationResponder(hipchat, logger))

  // vvv TODO: extract into Command class vvv
  /**
   * A Sauna command.
   *
   * @param envelope Identifies the command and provides instructions on how to execute it.
   * @param command  Specifies the exact action to execute.
   */
  case class SaunaCommand(
    envelope: SelfDescribingData[JsValue],
    command: SelfDescribingData[JsValue])

  implicit val schemaKeyReads: Reads[SchemaKey] = Reads {
    case JsString(s) => SchemaKey.fromUri(s) match {
      case Some(key) => JsSuccess(key)
      case None => JsError(s"Invalid SchemaKey: $s")
    }
    case _ => JsError("Non-string SchemaKey")
  }

  implicit val selfDescribingDataReads: Reads[SelfDescribingData[JsValue]] = (
    (JsPath \ "schema").read[SchemaKey] and
      (JsPath \ "data").read[JsValue]
    ) (SelfDescribingData.apply(_, _))

  implicit val saunaCommandReads: Reads[SaunaCommand] = (
    (JsPath \ "envelope").read[SelfDescribingData[JsValue]] and
      (JsPath \ "command").read[SelfDescribingData[JsValue]]
    ) (SaunaCommand.apply _)

  sealed trait Semantics
  case object AT_LEAST_ONCE extends Semantics
  object Semantics {
    val values = List(AT_LEAST_ONCE)
  }

  implicit val semanticsReads: Reads[Semantics] = Reads {
    case JsString(s) => Semantics.values.find(value => value.toString == s) match {
      case Some(value) => JsSuccess(value)
      case None => JsError(s"Invalid Semantics: $s")
    }
    case _ => JsError("Non-string Semantics")
  }

  /**
   * TODO: define what this is
   *
   * @param semantics  TODO: define what this is
   * @param timeToLive The command's lifetime (in ms): if a command is
   *                   received after `timeToLive` ms from its' creation have
   *                   passed, it will be discarded. (If None, a command won't be
   *                   discarded based on creation time.)
   */
  case class ExecutionParams(
    semantics: Semantics,
    timeToLive: Option[Int]
  )

  implicit val timeToLiveReads: Reads[Option[Int]] = JsPath.readNullable[Int]
  implicit val executionParamsReads: Reads[ExecutionParams] = Json.reads[ExecutionParams]

  /**
   * A Sauna command envelope.
   *
   * @param commandId   A unique identifier (uuid4) for the command.
   * @param whenCreated The command's creation date.
   * @param execution   TODO: define what this is
   * @param tags        TODO: define what this is
   */
  case class CommandEnvelope(
    commandId: String,
    whenCreated: LocalDateTime,
    execution: ExecutionParams,
    tags: Map[String, String]
  )
  implicit val commandEnvelopeReads: Reads[CommandEnvelope] = Json.reads[CommandEnvelope]

  /**
   * Attempts to extract a Sauna command from a [[JsValue]].
   *
   * @param json The [[JsValue]] to extract a command from.
   * @tparam T The type of the command's data.
   * @return Right containing a tuple of the command's envelope and data
   *         if the extraction was successful, Left containing an error message
   *         otherwise.
   */
  def extractCommand[T](json: JsValue)(implicit tReads: Reads[T]): Either[String, (CommandEnvelope, T)] = {
    val jsResult = for {
      s <- json.validate[SelfDescribingData[JsValue]]
      c <- s.data.validate[SaunaCommand]
      e <- c.envelope.data.validate[CommandEnvelope]
      d <- c.command.data.validate[T]
    } yield (e, d)

    jsResult.asEither match {
      case Right(success) => Right(success)
      case Left(errors) => Left(s"[Common error message]: $errors")
    }
  }

  /**
   * Processes a Sauna command envelope.
   *
   * @param envelope A Sauna command envelope.
   * @return None if the envelope was successfully processed
   *         and the command's data can be executed,
   *         Some containing an error message otherwise.
   */
  def processEnvelope(envelope: CommandEnvelope): Option[String] = {
    for {
      ms <- envelope.execution.timeToLive
      commandLife = envelope.whenCreated.until(LocalDateTime.now(), ChronoUnit.MILLIS)
      if commandLife > ms
    } yield s"Command has expired: time to live is $ms but $commandLife has passed"
  }
}