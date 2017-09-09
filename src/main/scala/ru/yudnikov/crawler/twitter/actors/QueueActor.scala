package ru.yudnikov.crawler.twitter.actors

import akka.actor.Actor
import QueueActor.{DequeueRequest, EnqueueRequest}
import ru.yudnikov.crawler.twitter.Waiter
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.trash.Loggable

import scala.collection.mutable

/**
  *
  * @param name
  * @param queueCapacity
  */
class QueueActor[T](name: String, queueCapacity: Int) extends Actor with Loggable {
  
  private val queue: mutable.Queue[T] = mutable.Queue()
  private var capacityOvercome = false
  
  override def receive: Receive = {
    case DequeueRequest(1) =>
      checkOverflow()
      if (queue.nonEmpty) {
        val res = queue.dequeue()
        logger.trace(s"dequeue $res")
        sender() ! Some(res)
      } else {
        sender() ! None
      }
    case DequeueRequest(n: Int) =>
      checkOverflow()
      val res = for {
        _ <- 1 to n
        if queue.nonEmpty
      } yield {
        val res = queue.dequeue()
        checkOverflow()
        res
      }
      if (res.nonEmpty) {
        sender() ! Some(res.toList)
      } else {
        sender() ! None
      }
    // TODO remove branch
    case EnqueueRequest(waiter: Waiter) =>
      logger.trace(s"enqueue $waiter")
      if (!capacityOvercome) {
        queue.enqueue(waiter.asInstanceOf[T])
        checkOverflow()
      } else {
        logger.debug(s"wouldn't enqueue, saving...")
        Cassandra.waitersQueueSave(name, mutable.Queue(waiter))
      }
    case EnqueueRequest(any@_*) =>
      logger.trace(s"enqueue $any")
      val seq = any.collect {
        case x: T => x
      }
      logger.trace(s"collected instances of T: $seq")
      // TODO probably need to handle situation when current queue length + waiters length makes much overflow but why?
      if (!capacityOvercome) {
        queue.enqueue(seq: _*)
        checkOverflow()
      } else {
        logger.debug(s"wouldn't enqueue, saving...")
        seq match {
          case s: Seq[Waiter] =>
            Cassandra.waitersQueueSave(name, s)
        }
      }
  }
  
  private def checkOverflow(): Unit = {
    if (!capacityOvercome && queue.size >= queueCapacity) {
      logger.trace(s"queue capacity overcome")
      capacityOvercome = true
    } else if (capacityOvercome && queue.isEmpty) {
      // TODO this may be atomic transaction with last batch delete mutation
      logger.trace(s"queue empty")
      queue match {
        case s: Seq[Waiter] =>
        
      }
      val q = Cassandra.waitersQueueLoad(name, queueCapacity)
      logger.trace(s"queue loaded: \n$q")
      queue ++= q.asInstanceOf[Seq[T]]
      capacityOvercome = false
      Cassandra.waitersQueueDelete(name, q)
    }
  }
  
  override def postStop(): Unit = {
    logger.info(s"terminating queue keeper")
    queue match {
      case q: mutable.Queue[Waiter] if q.nonEmpty && q.head.isInstanceOf[Waiter] =>
        Cassandra.waitersQueueSave(name, q)
      case q: mutable.Queue[Long] if q.nonEmpty && q.head.isInstanceOf[Long] =>
        Cassandra.longsQueueSave(name, q)
      case _ =>
        logger.warn(s"unmatched case")
    }
    super.postStop()
  }
  
}

object QueueActor {
  
  case class EnqueueRequest(values: Any*)
  
  case class DequeueRequest(n: Int)
  
}