package ru.yudnikov.crawler.twitter.actors

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props}
import ru.yudnikov.crawler.twitter.actors.PerformanceCounterActor.PerformanceCountResponse
import CollectorActor.{CollectDataResponse, CollectIDsResponse}
import DispatcherActor.StartMessage
import org.json4s.native.Serialization
import ru.yudnikov.crawler.twitter.enums.{Collectibles, Markers}
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.crawler.twitter.utils.{Loggable, TwitterUtils, Utils}
import ru.yudnikov.crawler.twitter.{Dependencies, Waiter}
import ru.yudnikov.crawler.twitter.actors.PerformanceCounterActor.PerformanceCountResponse
import twitter4j._

import scala.collection.JavaConverters._

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
    
    val scheduler = Dependencies.actorSystem.scheduler
    
    var i = 1
    
    Dependencies.twitters.foreach { twitter =>
  
      val idsInterval = 60500
      val r1 = Dependencies.random.nextInt(idsInterval).millis
  
      def scheduleFollowersIDsCollector(): Unit = {
        
        def followersFunction: (Twitter, Waiter) => Any = (twitter, waiter: Waiter) =>
          twitter.getFollowersIDs(waiter.id, waiter.cursor)
        
        val actorRef = context.actorOf(Props(classOf[CollectorActor[Waiter]], queues(Collectibles.FOLLOWERS), twitter, followersFunction, Collectibles.FOLLOWERS, 1), s"followers-collector-$i")
        
        scheduler.schedule(r1, idsInterval.millis, actorRef, CollectorActor.CollectRequest)
      }
      
      scheduleFollowersIDsCollector()
  
      def scheduleFriendsIDsCollector(): Unit = {
  
        // friends ids collector
        def friendsFunction: (Twitter, Waiter) => Any = (twitter, waiter: Waiter) =>
          twitter.getFriendsIDs(waiter.id, waiter.cursor)
        
        val actor = context.actorOf(Props(classOf[CollectorActor[Waiter]], queues(Collectibles.FRIENDS), twitter, friendsFunction, Collectibles.FRIENDS, 1), s"friends-collector-$i")
        
        scheduler.schedule(r1, 61.seconds, actor, CollectorActor.CollectRequest)
      }
      
      scheduleFriendsIDsCollector()
      
      val lookupInterval = 3100
      val r2 = Dependencies.random.nextInt(lookupInterval).millis
  
      def scheduleDataCollector(): Unit = {
        
        def lookupFunction: (Twitter, List[Long]) => Any = (twitter, longs) =>
          twitter.users().lookupUsers(longs: _*)
  
        // need to wait 1 iteration for 1st twitter
        val initDelay = if (i == 1) lookupInterval.millis + r2 else r2
        val dataCollector = context.actorOf(Props(classOf[CollectorActor[List[Long]]], queues(Collectibles.LOOKUP), twitter, lookupFunction, Collectibles.LOOKUP, 100), s"data-collector-$i")
        
        scheduler.schedule(initDelay, lookupInterval.millis, dataCollector, CollectorActor.CollectRequest)
      }
      
      scheduleDataCollector()
      
      i = i + 1
    }
    
    val performanceCounter = context.actorOf(Props(classOf[PerformanceCounterActor]))
    //performanceCounter ! PerformanceCounterActor.PerformanceCountRequest
    scheduler.schedule(0.seconds, 1.minute, performanceCounter, PerformanceCounterActor.PerformanceCountRequest)
    
  }
  
  private def enqueue0(twitter: Twitter): Unit = {
    // TODO pass this to kick-start
    val ids0 = Dependencies.config.getLongList(s"twitter.startIDs").asScala.toList.map(_.toLong)
    val screenNames = Dependencies.config.getStringList(s"twitter.startPages").asScala.map(TwitterUtils.screenNameFromURL).toList
    assume(screenNames.length <= 100)
    val idsFromScreenNames = twitter.users().lookupUsers(screenNames: _*).asScala.map(_.getId).toList
    val ids = ids0 ::: idsFromScreenNames
    val waiters = ids.map(long => Waiter(long))
    queues(Collectibles.FOLLOWERS) ! QueueActor.EnqueueRequest(waiters: _*)
    queues(Collectibles.FRIENDS) ! QueueActor.EnqueueRequest(waiters: _*)
    queues(Collectibles.LOOKUP) ! QueueActor.EnqueueRequest(ids: _*)
  }
  
  override def receive: Receive = {
    case StartMessage =>
      logger.trace(Markers.DISPATCHING, s"received start message")
      enqueue0(Dependencies.twitters.head)
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
    case PerformanceCountResponse(seconds, result) =>
      logger.info(Markers.PERFORMANCE, s"at the moment of $seconds collected: ${result.mkString("\n\t", "\n\t", "")}")
      implicit val formats = org.json4s.DefaultFormats
      val json = Serialization.write(result.map(t => t._1.toString -> t._2))
      Cassandra.performanceSave(seconds, json)
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