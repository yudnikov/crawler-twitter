package ru.yudnikov.crawler.twitter.actors

import akka.actor.Actor
import QueueActor.{DequeueRequest, EnqueueRequest}
import ru.yudnikov.crawler.twitter.{Dependencies, Waiter}
import ru.yudnikov.crawler.twitter.enums.Collectibles.Collectibles
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.crawler.twitter.utils.Loggable

import scala.collection.mutable

/** Stateful actor who keeps a queue
  *
  * @param name collectible enum value
  * @param queueCapacity queue length after which data sends to disk
  */
class QueueActor[T](name: Collectibles, queueCapacity: Int, cassandra: Cassandra) extends Actor with Loggable {
  
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
        cassandra.waitersQueueSave(name, mutable.Queue(waiter))
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
          case s: Seq[Waiter] if s.nonEmpty && seq.head.isInstanceOf[Waiter] =>
            cassandra.waitersQueueSave(name, s)
          case s: Seq[Long] if s.nonEmpty && seq.head.isInstanceOf[Long] =>
            cassandra.longsQueueSave(name, s)
          case _ =>
            logger.warn(s"unmatched case")
        }
      }
  }
  
  /** Checks if queue is overflow and sets respective variable */
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
      val q = cassandra.waitersQueueLoad(name, queueCapacity)
      logger.trace(s"queue loaded: \n$q")
      queue ++= q.asInstanceOf[Seq[T]]
      capacityOvercome = false
      cassandra.waitersQueueDelete(name, q)
    }
  }
  
  /** Saves current queue to disk */
  override def postStop(): Unit = {
    logger.info(s"terminating queue keeper")
    queue match {
      case q: mutable.Queue[Waiter] if q.nonEmpty && q.head.isInstanceOf[Waiter] =>
        cassandra.waitersQueueSave(name, q)
      case q: mutable.Queue[Long] if q.nonEmpty && q.head.isInstanceOf[Long] =>
        cassandra.longsQueueSave(name, q)
      case _ =>
        logger.warn(s"unmatched case")
    }
    super.postStop()
  }
  
}

/** Messages */
object QueueActor {
  
  /** Request to put some values into queue */
  case class EnqueueRequest(values: Any*)
  
  /** Poll request */
  case class DequeueRequest(n: Int)
  
  // TODO define response message formats!
  
}