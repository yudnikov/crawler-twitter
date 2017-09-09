package ru.yudnikov.crawler.twitter.actors

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props}
import ru.yudnikov.crawler.twitter.actors.PerformanceCounterActor.PerformanceCountResponse
import CollectorActor.{CollectDataResponse, CollectIDsResponse}
import DispatcherActor.StartMessage
import ru.yudnikov.crawler.twitter.enums.{Collectibles, Markers}
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.crawler.twitter.utils.{TwitterUtils, Utils}
import ru.yudnikov.crawler.twitter.utils.Utils
import ru.yudnikov.crawler.twitter.Waiter
import ru.yudnikov.crawler.twitter.actors.PerformanceCounterActor.PerformanceCountResponse
import ru.yudnikov.trash.Loggable
import ru.yudnikov.trash.twitter.Dependencies
import twitter4j._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by Don on 07.09.2017.
  */
class DispatcherActor extends Actor with Loggable {
  
  val queues = Map(
    Collectibles.FOLLOWERS -> context.actorOf(Props(classOf[QueueActor[Waiter]], "followers", 5000)),
    Collectibles.FRIENDS -> context.actorOf(Props(classOf[QueueActor[Waiter]], "friends", 5000)),
    Collectibles.LOOKUP -> context.actorOf(Props(classOf[QueueActor[Long]], "lookup", 5000))
  )
  
  def start(): Unit = {
    
    //queues()
    val twitters = Utils.getMapsFromConfig(Dependencies.config, "twitter.OAuths").map { map =>
      TwitterUtils.getTwitter(map.asInstanceOf[Map[String, String]])
    }
  
    val scheduler = Dependencies.actorSystem.scheduler
    
    var i = 1
    twitters.foreach { twitter =>
      
      // followers ids collector
      def followersFunction: (Twitter, Waiter) => Any = (twitter, waiter: Waiter) =>
        twitter.getFollowersIDs(waiter.id, waiter.cursor)
      
      val r = Dependencies.random.nextInt(60000)

      val followersCollector = context.actorOf(Props(classOf[CollectorActor[Waiter]], queues(Collectibles.FOLLOWERS), twitter, followersFunction, Collectibles.FOLLOWERS, 1), s"followers-collector-$i")
      scheduler.schedule(r.millis, 61.seconds, followersCollector, CollectorActor.CollectRequest)
      
      // friends ids collector
      def friendsFunction: (Twitter, Waiter) => Any = (twitter, waiter: Waiter) =>
        twitter.getFriendsIDs(waiter.id, waiter.cursor)
      
      val friendsCollector = context.actorOf(Props(classOf[CollectorActor[Waiter]], queues(Collectibles.FRIENDS), twitter, friendsFunction, Collectibles.FRIENDS, 1), s"friends-collector-$i")
      scheduler.schedule(r.millis, 61.seconds, friendsCollector, CollectorActor.CollectRequest)
      
      def lookupFunction: (Twitter, List[Long]) => Any = (twitter, longs) => {
        twitter.users().lookupUsers(longs: _*)
      }
      val dataCollector = context.actorOf(Props(classOf[CollectorActor[List[Long]]], queues(Collectibles.LOOKUP), twitter, lookupFunction, Collectibles.LOOKUP, 100), s"data-collector-$i")
      scheduler.schedule(r.millis, 3.2.seconds, dataCollector, CollectorActor.CollectRequest)
      
      i = i + 1
    }
    
    val performanceCounter = context.actorOf(Props(classOf[PerformanceCounterActor]))
    
    performanceCounter ! PerformanceCounterActor.PerformanceCountRequest
    
    scheduler.schedule(0.seconds, 1.minute, performanceCounter, PerformanceCounterActor.PerformanceCountRequest)
    
  }
  
  private def enqueue0(): Unit = {
    // TODO pass this to kick-start
    val ids0 = List(Waiter(905662195106627584L), Waiter(865495825211768837L), Waiter(2350536240L), Waiter(2966223190L))
    queues(Collectibles.FOLLOWERS) ! QueueActor.EnqueueRequest(ids0: _*)
    queues(Collectibles.FRIENDS) ! QueueActor.EnqueueRequest(ids0: _*)
    queues(Collectibles.LOOKUP) ! QueueActor.EnqueueRequest(ids0.map(_.id): _*)
  }
  
  override def receive: Receive = {
    case StartMessage =>
      logger.trace(Markers.DISPATCHING, s"received start message")
      enqueue0()
      start()
    case CollectIDsResponse(name, source: Waiter, ids, maybeNext) => {
      logger.debug(Markers.DISPATCHING, s"recived collect ids response:" +
        s"\tsource: $source\n" +
        s"\tids.size: ${ids.size}\n" +
        s"\tmaybeNext: $maybeNext")
      Cassandra.idsSave(name, source.id, source.cursor, ids.map(_.id))
      maybeNext match {
        case Some(waiter) =>
          queues(name) ! QueueActor.EnqueueRequest(waiter)
        case _ =>
      }
      if (ids.length > 1000) {
        logger.warn(Markers.DISPATCHING, s"ids 1000+ received")
      }
      val idsLongs = ids.map(_.id)
      val nonExisting = Cassandra.membersNonExistingSpark(idsLongs: _*)
      Cassandra.membersInsert(nonExisting: _*)
      val nonExistingWaiters = ids.filter(follower => nonExisting.contains(follower.id))
      queues(name) ! QueueActor.EnqueueRequest(nonExistingWaiters: _*)
      queues(Collectibles.LOOKUP) ! QueueActor.EnqueueRequest(nonExistingWaiters.map(_.id): _*)
    }
    
    case CollectDataResponse(name, sources, data) =>
      logger.debug(Markers.DISPATCHING, s"recived collect data response: \n" +
        s"\tname = $name\n" +
        s"\tsources = $sources\n" +
        s"\tdata = $data")
      Cassandra.lookupSave(data)
    case PerformanceCountResponse(date, result) =>
      logger.info(Markers.PERFORMANCE, s"at the moment of $date collected: ${result.mkString("\n\t", "\n\t", "")}")
    case s: String =>
      logger.trace(Markers.DISPATCHING, s"received string: \n\t$s")
  }
  
  override def postStop(): Unit = {
    logger.trace(Markers.DISPATCHING, s"stopping dispatcher")
  }
}

object DispatcherActor {
  
  object StartMessage
  
}