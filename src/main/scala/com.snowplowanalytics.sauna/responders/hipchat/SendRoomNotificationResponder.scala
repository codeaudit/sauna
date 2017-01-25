package com.snowplowanalytics.sauna
package responders
package hipchat

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor.{ActorRef, Props}
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.sauna.apis.Hipchat
import com.snowplowanalytics.sauna.apis.Hipchat.RoomNotification
import com.snowplowanalytics.sauna.loggers.Logger.Notification
import com.snowplowanalytics.sauna.observers.Observer.ObserverBatchEvent
import com.snowplowanalytics.sauna.responders.Responder.{ResponderEvent, ResponderResult}
import com.snowplowanalytics.sauna.responders.hipchat.SendRoomNotificationResponder.Semantics.Semantics
import com.snowplowanalytics.sauna.responders.hipchat.SendRoomNotificationResponder.{CommandEnvelope, RoomNotificationReceived, SaunaCommand}
import play.api.libs.json._

import scala.io.Source


class SendRoomNotificationResponder(hipchat: Hipchat, val logger: ActorRef) extends Responder[RoomNotificationReceived] {

  override def extractEvent(observerEvent: ObserverBatchEvent): Option[RoomNotificationReceived] = {
    // TODO: extract most of this elsewhere
    observerEvent.streamContent match {
      case Some(is) =>
        val commandJson = Json.parse(Source.fromInputStream(is).mkString)
        commandJson.validate[SaunaCommand] match {
          case JsSuccess(command, _) =>
            command.envelope.data.validate[CommandEnvelope] match {
              case JsSuccess(envelope, _) =>
                envelope.execution.timeToLive match {
                  case Some(ms) =>
                    val commandLife = envelope.whenCreated.until(LocalDateTime.now(), ChronoUnit.MILLIS)
                    if (commandLife < ms) {
                      logger ! Notification(s"Command has expired: time to live is $ms but $commandLife elapsed")
                      None
                    }
                }
                command.command.data.validate[RoomNotification] match {
                  case JsSuccess(notification, _) =>
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

  override def process(event: RoomNotificationReceived): Unit =
    hipchat.sendRoomNotification(event.roomNotification)
}

object SendRoomNotificationResponder {
  case class RoomNotificationReceived(
    roomNotification: RoomNotification,
    source: ObserverBatchEvent) extends ResponderEvent[ObserverBatchEvent]

  case class RoomNotificationSent(
    source: RoomNotificationReceived,
    message: String) extends ResponderResult

  case class SelfDescribing(
    schema: SchemaKey,
    data: JsValue)

  case class SaunaCommand(
    envelope: SelfDescribing,
    command: SelfDescribing)

  case class CommandEnvelope(
    commandId: String,
    whenCreated: LocalDateTime,
    execution: ExecutionParams,
    tags: Map[String, String]
  )

  case class ExecutionParams(
    semantics: Semantics,
    timeToLive: Option[Integer]
  )

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