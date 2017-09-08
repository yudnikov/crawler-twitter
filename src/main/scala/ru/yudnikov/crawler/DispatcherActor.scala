package ru.yudnikov.crawler

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props, Scheduler}
import com.typesafe.config.Config
import ru.yudnikov.crawler.IDsCollectorActor.{CollectIDsResponse}
import ru.yudnikov.crawler.DispatcherActor.StartMessage
import ru.yudnikov.trash.Loggable
import ru.yudnikov.trash.twitter.{Cassandra, Dependencies}
import ru.yudnikov.trash.twitter.Dependencies.config
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{IDs, Twitter, TwitterFactory}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by Don on 07.09.2017.
  */
class DispatcherActor(scheduler: Scheduler) extends Actor with Loggable {
  
  val queues = Map(
    "followers" -> context.actorOf(Props(classOf[QueueKeeperActor], "followers")),
    "friends" -> context.actorOf(Props(classOf[QueueKeeperActor], "friends"))
  
  )
  
  def start(): Unit = {
    // TODO pass this to kick-start
    queues("followers") ! QueueKeeperActor.EnqueueRequest(Waiter(905662195106627584L), Waiter(865495825211768837L), Waiter(2350536240L), Waiter(2966223190L))
    queues("friends") ! QueueKeeperActor.EnqueueRequest(Waiter(905662195106627584L), Waiter(865495825211768837L), Waiter(2350536240L), Waiter(2966223190L))
    val twitters = Utils.getMapsFromConfig(Dependencies.config, "twitter.OAuths").map { map =>
      TwitterUtils.getTwitter(map.asInstanceOf[Map[String, String]])
    }
    twitters.map { twitter =>
      // followers ids collector
      def getFollowersIDs: (Twitter, Long, Long) => IDs = (twitter, id, count) => twitter.getFollowersIDs(id, count)
      
      val r = Dependencies.random.nextInt(60000)
      val initDelay = FiniteDuration(r, TimeUnit.MILLISECONDS)
      val followersCollector = context.actorOf(Props(classOf[IDsCollectorActor], queues("followers"), twitter, getFollowersIDs, "followers"), s"followers-collector-$r")
      
      // TODO 61 seconds interval
      val interval = FiniteDuration(61, TimeUnit.SECONDS)
      Dependencies.actorSystem.scheduler.schedule(initDelay, interval, followersCollector, IDsCollectorActor.CollectIDsRequest)
      
      // friends ids collector
      def getFriendsIDs: (Twitter, Long, Long) => IDs = (twitter, id, count) => twitter.getFriendsIDs(id, count)
      
      val friendsCollector = context.actorOf(Props(classOf[IDsCollectorActor], queues("friends"), twitter, getFriendsIDs, "friends"), s"friends-collector-$r")
      Dependencies.actorSystem.scheduler.schedule(initDelay, interval, friendsCollector, IDsCollectorActor.CollectIDsRequest)
    }
  }
  
  override def receive: Receive = {
    case StartMessage =>
      logger.trace(s"received start message")
      start()
    case CollectIDsResponse(relationType, source, ids, maybeNext) => {
      logger.debug(s"handling collect ids response:" +
        s"\tsource: $source" +
        s"\tids.size: ${ids.size}" +
        s"\tmaybeNext: $maybeNext")
      Cassandra.idsSave(relationType, source.id, source.cursor, ids.map(_.id))
      
      maybeNext match {
        case Some(waiter) =>
          queues(relationType) ! QueueKeeperActor.EnqueueRequest(waiter)
        case _ =>
      }
      if (ids.length > 1000) {
        logger.warn(s"ids 1000+ received")
      }
      val nonExisting = Cassandra.membersNonExisting(ids.map(_.id): _*)
      Cassandra.membersInsert(nonExisting: _*)
      val nonExistingWaiters = ids.filter(follower => nonExisting.contains(follower.id))
      //ids.filter(follower => nonExisting.contains(follower.id)).foreach { waiter =>
      queues(relationType) ! QueueKeeperActor.EnqueueRequest(nonExistingWaiters: _*)
      //}
      //null
    }
    case s: String =>
      logger.trace(s"received string: \n\t$s")
  }
  
  override def postStop(): Unit = {
    logger.trace(s"stopping")
  }
}

object DispatcherActor {
  
  object StartMessage
  
}