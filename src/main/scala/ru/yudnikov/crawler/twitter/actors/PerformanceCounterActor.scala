package ru.yudnikov.crawler.twitter.actors

import akka.actor.Actor
import org.joda.time.{DateTime, Seconds}
import PerformanceCounterActor.{PerformanceCountRequest, PerformanceCountResponse}
import ru.yudnikov.crawler.twitter.enums.Collectibles
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.crawler.twitter.utils.Loggable

/**
  * Created by Don on 09.09.2017.
  */
class PerformanceCounterActor extends Actor with Loggable {
  
  val countable = List(Collectibles.MEMBERS, Collectibles.FRIENDS, Collectibles.FOLLOWERS, Collectibles.LOOKUP)
  
  private var startedLatch: Boolean = false
  
  private var previous: (DateTime, Map[Collectibles.Value, Long]) =
    new DateTime -> countable.map(v => v -> Cassandra.tableCount(v)).toMap
  
  override def receive: Receive = {
    case PerformanceCountRequest =>
      if (!startedLatch) {
        startedLatch = true
      } else {
        val currentCheck = new DateTime() -> countable.map(v => v -> Cassandra.tableCount(v)).toMap
        val answer = Seconds.secondsBetween(previous._1, currentCheck._1) -> currentCheck._2.map { current =>
          current._1 -> (current._2 - previous._2(current._1))
        }
        previous = currentCheck
        sender() ! PerformanceCountResponse(answer._1.getSeconds, answer._2)
      }
    case _ =>
      logger.debug(s"")
  }
}

object PerformanceCounterActor {
  
  object PerformanceCountRequest
  
  case class PerformanceCountResponse(seconds: Int, result: Map[Collectibles.Value, Long])
  
}
