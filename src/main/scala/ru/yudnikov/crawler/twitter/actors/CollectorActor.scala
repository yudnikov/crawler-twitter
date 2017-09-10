package ru.yudnikov.crawler.twitter.actors

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import ru.yudnikov.crawler.twitter.Waiter
import ru.yudnikov.crawler.twitter.actors.CollectorActor.{CollectDataResponse, CollectIDsResponse, CollectRequest}
import ru.yudnikov.crawler.twitter.enums.Collectibles
import ru.yudnikov.crawler.twitter.utils.Loggable
import twitter4j._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** Collector actor
  *
  * The primary worker actor of the solution
  * - receives collect request from dispatcher, asks queue actor for next waiter and if receives answer, performs
  * twitter api request, and returns the result to dispatcher
  *
  * @param queueKeeper queue actor reference
  * @param twitter twitter api instance
  * @param f function twitter api => result
  * @param name collectible enum value
  * @param collectBy size of queue request
  * @tparam T generic type of waiter (waiter or long)
  */
class CollectorActor[T](queueKeeper: ActorRef, twitter: Twitter, f: (Twitter, T) => Any, name: Collectibles.Value,
                        collectBy: Int = 1) extends Actor with Loggable {
  
  implicit val timeout = Timeout(60, TimeUnit.SECONDS)
  
  override def receive: Receive = {
    case CollectRequest =>
      logger.trace(s"received collect IDs message")
      val futureMaybeT = (queueKeeper ? QueueActor.DequeueRequest(collectBy)).asInstanceOf[Future[Option[T]]]
      val maybeWaiter = Await.result(futureMaybeT, Duration.Inf)
      logger.trace(s"received maybeWaiter $maybeWaiter")
      maybeWaiter match {
        case Some(t: T) =>
          val result = try {
            Some(f(twitter, t.asInstanceOf[T]))
          } catch {
            case e: Exception =>
              logger.error(s"can't get followers by id", e)
              None
          }
          // TODO probably it's better to encapsulate this logic out of here and just return result
          val answer = t match {
            case waiter: Waiter =>
              result match {
                case Some(ids: IDs) if ids.hasNext =>
                  val followers = ids.getIDs.toList.map(id => Waiter(id))
                  val nextWaiter = Some(Waiter(waiter.id, ids.getNextCursor))
                  CollectIDsResponse(name, waiter, followers, nextWaiter)
                case Some(ids: IDs) =>
                  val followers = ids.getIDs.toList.map(id => Waiter(id))
                  CollectIDsResponse(name, waiter, followers)
                case _ =>
                  CollectIDsResponse(name, waiter, Nil)
              }
            case longs: List[Long] =>
              result match {
                case Some(users: ResponseList[User]) =>
                  val data = users.iterator().asScala map { user =>
                    user.getId -> TwitterObjectFactory.getRawJSON(user)
                  }
                  CollectDataResponse(name, longs, data.toMap)
              }
          }
          sender() ! answer
        case _ =>
          logger.debug("no waiter received, nothing to answer")
      }
  }
  
}

/** Messages */
object CollectorActor {
  
  /** Message to initiate data collecting */
  object CollectRequest
  
  /** Message class of response followers or friends
    *
    * @param name collectible enum value
    * @param source source id or waiter
    * @param waiters a feed of retrieved ids wrapped as waiters
    * @param maybeNext if the feed in tot the tail maybeNext should be set to Some(Waiter(_, cursor))
    */
  case class CollectIDsResponse(name: Collectibles.Value, source: Waiter, waiters: List[Waiter],
                                maybeNext: Option[Waiter] = None)
  
  /** Message to return result of lookup
    *
    * @param name collectible enum value
    * @param sources list of sources, probable excess and could be replaced by data.values
    * @param data Map[Long, String] where key is source and value json string or lookup result
    */
  case class CollectDataResponse(name: Collectibles.Value, sources: List[Long], data: Map[Long, String])
  
}