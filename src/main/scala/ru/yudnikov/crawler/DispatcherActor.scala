package ru.yudnikov.crawler

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props, Scheduler}
import com.typesafe.config.Config
import ru.yudnikov.crawler.CollectorActor.{CollectDataResponse, CollectIDsResponse}
import ru.yudnikov.crawler.DispatcherActor.StartMessage
import ru.yudnikov.crawler.queues.QueueActor
import ru.yudnikov.trash.Loggable
import ru.yudnikov.trash.twitter.{Cassandra, Dependencies}
import ru.yudnikov.trash.twitter.Dependencies.config
import twitter4j.conf.ConfigurationBuilder
import twitter4j._

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by Don on 07.09.2017.
  */
class DispatcherActor extends Actor with Loggable {
  
  val queues = Map(
    "followers" -> context.actorOf(Props(classOf[QueueActor[Waiter]], "followers", 10000)),
    "friends" -> context.actorOf(Props(classOf[QueueActor[Waiter]], "friends", 10000)),
    "lookup" -> context.actorOf(Props(classOf[QueueActor[Long]], "lookup", 10000))
  )
  
  def start(): Unit = {
    // TODO pass this to kick-start
    val ids0 = List(Waiter(905662195106627584L), Waiter(865495825211768837L), Waiter(2350536240L), Waiter(2966223190L))
    queues("followers") ! QueueActor.EnqueueRequest(ids0: _*)
    queues("friends") ! QueueActor.EnqueueRequest(ids0: _*)
    queues("lookup") ! QueueActor.EnqueueRequest(ids0.map(_.id): _*)
    //queues()
    val twitters = Utils.getMapsFromConfig(Dependencies.config, "twitter.OAuths").map { map =>
      TwitterUtils.getTwitter(map.asInstanceOf[Map[String, String]])
    }
    var i = 1
    twitters.foreach { twitter =>
      
      val scheduler = Dependencies.actorSystem.scheduler
      
      // followers ids collector
      def followersFunction: (Twitter, Waiter) => Any = (twitter, waiter: Waiter) =>
        twitter.getFollowersIDs(waiter.id, waiter.cursor)
      
      val r = Dependencies.random.nextInt(60000)
      val initDelay = FiniteDuration(r, TimeUnit.MILLISECONDS)
      val followersCollector = context.actorOf(Props(classOf[CollectorActor[Waiter]], queues("followers"), twitter, followersFunction, "followers", 1), s"followers-collector-$i")
      // TODO 61 seconds interval
      val interval = FiniteDuration(61, TimeUnit.SECONDS)
      scheduler.schedule(initDelay, interval, followersCollector, CollectorActor.CollectRequest)
      
      // friends ids collector
      def friendsFunction: (Twitter, Waiter) => Any = (twitter, waiter: Waiter) =>
        twitter.getFriendsIDs(waiter.id, waiter.cursor)
      
      val friendsCollector = context.actorOf(Props(classOf[CollectorActor[Waiter]], queues("friends"), twitter, friendsFunction, "friends", 1), s"friends-collector-$i")
      scheduler.schedule(initDelay, interval, friendsCollector, CollectorActor.CollectRequest)
      
      def lookupFunction: (Twitter, List[Long]) => Any = (twitter, longs) => {
        twitter.users().lookupUsers(longs: _*)
      }
      val dataCollector = context.actorOf(Props(classOf[CollectorActor[List[Long]]], queues("lookup"), twitter, lookupFunction, "lookup", 100), s"data-collector-$i")
      scheduler.schedule(initDelay, FiniteDuration(3500, TimeUnit.MILLISECONDS), dataCollector, CollectorActor.CollectRequest)
      
      i = i + 1
    }
  }
  
  override def receive: Receive = {
    case StartMessage =>
      logger.trace(s"received start message")
      start()
    case CollectIDsResponse(name, source: Waiter, ids, maybeNext) => {
      logger.debug(s"handling collect ids response:" +
        s"\tsource: $source" +
        s"\tids.size: ${ids.size}" +
        s"\tmaybeNext: $maybeNext")
      Cassandra.idsSave(name, source.id, source.cursor, ids.map(_.id))
      maybeNext match {
        case Some(waiter) =>
          queues(name) ! QueueActor.EnqueueRequest(waiter)
        case _ =>
      }
      if (ids.length > 1000) {
        logger.warn(s"ids 1000+ received")
      }
      val idsLongs = ids.map(_.id)
      //val nonExisting = Cassandra.membersNonExisting(idsLongs: _*)
      val nonExistingSpark = Cassandra.membersNonExistingSpark(idsLongs: _*)
      Cassandra.membersInsert(nonExistingSpark: _*)
      val nonExistingWaiters = ids.filter(follower => nonExistingSpark.contains(follower.id))
      queues(name) ! QueueActor.EnqueueRequest(nonExistingWaiters: _*)
      queues("lookup") ! QueueActor.EnqueueRequest(nonExistingWaiters.map(_.id): _*)
    }
    
    case CollectDataResponse(name, sources, data) =>
      println(s"recived collect data response")
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