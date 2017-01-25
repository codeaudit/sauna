package com.snowplowanalytics.sauna
package responders
package hipchat

// java
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import scala.util.{Failure, Success}

// scala
import scala.io.Source

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.json._

// iglu
import com.snowplowanalytics.iglu.core.SchemaKey

// sauna
import SendRoomNotificationResponder._
import apis.Hipchat
import apis.Hipchat._
import loggers.Logger.Notification
import observers.Observer.ObserverBatchEvent
import responders.Responder._

// TODO: there might be a better way to import enums
import hipchat.SendRoomNotificationResponder.Semantics.Semantics

class SendRoomNotificationResponder(hipchat: Hipchat, val logger: ActorRef) extends Responder[RoomNotificationReceived] {

  /**
   * Extracts a command from an observer event's stream and validates it.
   *
   * @param observerEvent An `ObserverBatchEvent` containing the command string within
   *                      its' `streamContent`.
   * @return A `RoomNotificationReceived` if the command is valid and needs to be
   *         processed by this responder, None if command is invalid or should be skipped.
   */
  override def extractEvent(observerEvent: ObserverBatchEvent): Option[RoomNotificationReceived] = {
    // TODO: extract most of this elsewhere.
    observerEvent.streamContent match {
      // Step 1. Retrieve an input stream.
      case Some(is) =>
        // Step 2. Retrieve the input stream as a string, then attempt
        // to deserialize (as JSON) to a Sauna command.
        val commandJson = Json.parse(Source.fromInputStream(is).mkString)
        commandJson.validate[SaunaCommand] match {
          case JsSuccess(command, _) =>
            // Step 3. Retrieve the envelope.
            command.envelope.data.validate[CommandEnvelope] match {
              case JsSuccess(envelope, _) =>
                // Step 4. Do envelope-related operations (validation etc.)
                // TODO: implement AT_LEAST_ONCE after most of this has been extracted elsewhere.
                envelope.execution.timeToLive match {
                  case Some(ms) =>
                    val commandLife = envelope.whenCreated.until(LocalDateTime.now(), ChronoUnit.MILLIS)
                    if (commandLife > ms) {
                      logger ! Notification(s"Command has expired: time to live is $ms but $commandLife has passed")
                      None
                    }
                }
                // Step 5. Retrieve the command.
                command.command.data.validate[RoomNotification] match {
                  case JsSuccess(notification, _) =>
                    // Step 6. Execute the command.
                    Some(RoomNotificationReceived(notification, observerEvent))
                  case JsError(error) =>
                    logger ! Notification(s"Encountered an issue while parsing Sauna command body: $error")
                    None
                }
              case JsError(error) =>
                logger ! Notification(s"Encountered an issue while parsing Sauna command envelope: $error")
                None
            }
          case JsError(error) =>
            logger ! Notification(s"Encountered an issue while parsing Sauna command: $error")
            None
        }
      case None =>
        logger ! Notification("No stream present, cannot parse command")
        None
    }
  }

  /**
   * Send a valid room notification using the API wrapper.
   *
   * @param event The event containing a room notification.
   */
  override def process(event: RoomNotificationReceived): Unit =
    hipchat.sendRoomNotification(event.roomNotification).onComplete {
      case Success(message) => context.parent ! RoomNotificationSent(event, "Successfully sent HipChat notification")
      case Failure(error) => logger ! Notification(s"Error while sending HipChat notification: $error")
    }
}

object SendRoomNotificationResponder {
  /**
   * A responder event denoting that a HipChat room notification was received by
   * the responder.
   *
   * @param roomNotification A room notification represented as a case class.
   * @param source           The observer event that triggered this responder event.
   */
  case class RoomNotificationReceived(
    roomNotification: RoomNotification,
    source: ObserverBatchEvent) extends ResponderEvent[ObserverBatchEvent]

  /**
   * A responder result denoting that a HipChat room notification was successfully sent
   * by the responder.
   *
   * @param source  The responder event that triggered this.
   * @param message A success message.
   */
  case class RoomNotificationSent(
    source: RoomNotificationReceived,
    message: String) extends ResponderResult

  /**
   * A self-describing JSON structure, containing a reference to a JSON
   * schema and data that should validate against the given schema.
   * TODO: validate SelfDescribing classes using iglu-scala-client when it's not broken.
   *
   * @param schema A self-describing schema key.
   * @param data   Data that should comply to the schema.
   */
  case class SelfDescribing(
    schema: SchemaKey,
    data: JsValue)

  /**
   * A Sauna command.
   *
   * @param envelope Identifies the command and provides instructions on how to execute it.
   * @param command  Specifies the exact action to execute.
   */
  case class SaunaCommand(
    envelope: SelfDescribing,
    command: SelfDescribing)

  /**
   * A Sauna command envelope.
   *
   * @param commandId   A unique identifier (uuid4) for the command.
   * @param whenCreated The command's creation date.
   * @param execution   TODO: explain what this is
   * @param tags        TODO: explain what this is
   */
  case class CommandEnvelope(
    commandId: String,
    whenCreated: LocalDateTime,
    execution: ExecutionParams,
    tags: Map[String, String]
  )

  /**
   * TODO: explain what this is
   *
   * @param semantics  TODO: explain what this is
   * @param timeToLive The command's lifetime (in ms): if a command is
   *                   received after `timeToLive` ms from its' creation have
   *                   passed, it will be discarded. (If None, a command won't be
   *                   discarded based on creation time.)
   */
  case class ExecutionParams(
    semantics: Semantics,
    timeToLive: Option[Integer]
  )

  /**
   * TODO: explain what this is
   */
  object Semantics extends Enumeration {
    type Semantics = Value
    val AT_LEAST_ONCE = Value
  }

  /**
   * Constructs a `Props` for a `SendRoomNotificationResponder` actor.
   *
   * @param hipchat The HipChat API wrapper.
   * @param logger  A logger actor.
   * @return `Props` for the new actor.
   */
  def props(hipchat: Hipchat, logger: ActorRef): Props =
    Props(new SendRoomNotificationResponder(hipchat, logger))
}