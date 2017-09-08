package ru.yudnikov.crawler

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import ru.yudnikov.crawler.IDsCollectorActor.{ CollectIDsRequest, CollectIDsResponse}
import ru.yudnikov.trash.Loggable
import ru.yudnikov.trash.twitter.Dependencies
import akka.pattern.ask
import akka.util.Timeout
import twitter4j.{IDs, Twitter}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by Don on 07.09.2017.
  */
class IDsCollectorActor(queueKeeper: ActorRef, twitter: Twitter, f: (Twitter, Long, Long) => IDs, relationType: String) extends Actor with Loggable {
  
  implicit val timeout = Timeout(60, TimeUnit.SECONDS)
  
  override def receive: Receive = {
    case CollectIDsRequest =>
      logger.trace(s"received collect message")
      val futureMaybeWaiter = (queueKeeper ? QueueKeeperActor.DequeueMessage).asInstanceOf[Future[Option[Waiter]]]
      val maybeWaiter = Await.result(futureMaybeWaiter, Duration.Inf)
      logger.trace(s"received maybeWaiter $maybeWaiter")
      maybeWaiter match {
        case Some(waiter) =>
          val result = try {
            Some(f(twitter, waiter.id, waiter.cursor))
          } catch {
            case e: Exception =>
              logger.error(s"can't get followers by id", e)
              None
          }
          val answer = result match {
            case Some(ids: IDs) if ids.hasNext =>
              CollectIDsResponse(relationType, waiter, ids.getIDs.toList.map(id => Waiter(id)), Some(Waiter(waiter.id, ids.getNextCursor)))
            case Some(ids: IDs) =>
              CollectIDsResponse(relationType, waiter, ids.getIDs.toList.map(id => Waiter(id)))
            case _ =>
              CollectIDsResponse(relationType, waiter, Nil)
          }
          sender() ! answer
        case _ =>
          logger.trace("no waiter received, nothing to answer")
      }
  }
  
}

object IDsCollectorActor {
  
  object CollectIDsRequest
  
  case class CollectIDsResponse(relationType: String, source: Waiter, followers: List[Waiter], maybeNext: Option[Waiter] = None)
  
}