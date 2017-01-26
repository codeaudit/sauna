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

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.functional.syntax._
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
   * @param observerEvent An [[ObserverBatchEvent]] containing the command string within
   *                      its' `streamContent`.
   * @return A [[RoomNotificationReceived]] if the command is valid and needs to be
   *         processed by this responder, None if command is invalid or should be skipped.
   */
  override def extractEvent(observerEvent: ObserverBatchEvent): Option[RoomNotificationReceived] = {
    observerEvent.streamContent match {
      case Some(is) =>
        val commandJson = Json.parse(Source.fromInputStream(is).mkString)
        extractCommand[RoomNotification](commandJson) match {
          case Right((envelope, data)) =>
            processEnvelope(envelope) match {
              case None =>
                Some(RoomNotificationReceived(data, observerEvent))
              case Some(error) =>
                logger ! Notification(error)
                None
            }
          case Left(error) =>
            logger ! Notification(error)
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
      case Success(message) => context.parent ! RoomNotificationSent(event, s"Successfully sent HipChat notification: $message")
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
   *
   * @param schema A self-describing schema key.
   * @param data   Data that should comply to the schema.
   */
  case class SelfDescribing(
    schema: SchemaKey,
    data: JsValue)

  implicit val selfDescribingReads: Reads[SelfDescribing] = (
    (JsPath \ "schema").read[String].map[SchemaKey](schemaKey => SchemaKey.fromUri(schemaKey).get) and
      (JsPath \ "data").read[JsValue]
    ) (SelfDescribing.apply _)

  /**
   * A Sauna command.
   *
   * @param envelope Identifies the command and provides instructions on how to execute it.
   * @param command  Specifies the exact action to execute.
   */
  case class SaunaCommand(
    envelope: SelfDescribing,
    command: SelfDescribing)

  implicit val saunaCommandReads: Reads[SaunaCommand] = (
    (JsPath \ "envelope").read[SelfDescribing] and
      (JsPath \ "command").read[SelfDescribing]
    ) (SaunaCommand.apply _)

  /**
   * TODO: define what this is
   */
  object Semantics extends Enumeration {
    type Semantics = Value
    val AT_LEAST_ONCE = Value("AT_LEAST_ONCE")
  }

  implicit val semanticsReads: Reads[Semantics] = Reads.enumNameReads(Semantics)

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
   * Constructs a [[Props]] for a [[SendRoomNotificationResponder]] actor.
   *
   * @param hipchat The HipChat API wrapper.
   * @param logger  A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(hipchat: Hipchat, logger: ActorRef): Props =
    Props(new SendRoomNotificationResponder(hipchat, logger))

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
    json.validate[SelfDescribing] match {
      case JsSuccess(selfDescribing, _) =>
        selfDescribing.data.validate[SaunaCommand] match {
          case JsSuccess(command, _) =>
            command.envelope.data.validate[CommandEnvelope] match {
              case JsSuccess(envelope, _) =>
                command.command.data.validate[T] match {
                  case JsSuccess(data, _) =>
                    Right((envelope, data))
                  case JsError(error) =>
                    Left(s"Encountered an issue while parsing Sauna command data: $error")
                }
              case JsError(error) =>
                Left(s"Encountered an issue while parsing Sauna command envelope: $error")
            }
          case JsError(error) =>
            Left(s"Encountered an issue while parsing Sauna command: $error")
        }
      case JsError(error) =>
        Left(s"Encountered an issue while parsing self-describing JSON: $error")
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
    envelope.execution.timeToLive match {
      case Some(ms) =>
        val commandLife = envelope.whenCreated.until(LocalDateTime.now(), ChronoUnit.MILLIS)
        if (commandLife > ms)
          return Some(s"Command has expired: time to live is $ms but $commandLife has passed")
      case None =>
    }
    None
  }
}