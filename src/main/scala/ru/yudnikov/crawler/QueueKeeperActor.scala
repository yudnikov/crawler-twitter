package ru.yudnikov.crawler

import akka.actor.Actor
import ru.yudnikov.crawler.QueueKeeperActor.{DequeueMessage, EnqueueRequest}
import ru.yudnikov.trash.Loggable
import ru.yudnikov.trash.twitter.Cassandra

import scala.collection.mutable

/**
  * Created by Don on 07.09.2017.
  */
class QueueKeeperActor(relationType: String) extends Actor with Loggable {
  
  private val queue: mutable.Queue[Waiter] = mutable.Queue()
  private val queueCapacity = 1000
  private var capacityOvercome = false
  
  override def receive: Receive = {
    case DequeueMessage =>
      checkOvercome()
      if (queue.nonEmpty) {
        val res = queue.dequeue()
        logger.trace(s"dequeue $res")
        sender() ! Some(res)
      } else {
        sender() ! None
      }
      // TODO remove branch
    case EnqueueRequest(waiter) =>
      logger.trace(s"enqueue $waiter")
      if (!capacityOvercome) {
        queue.enqueue(waiter)
        checkOvercome()
      } else {
        logger.debug(s"wouldn't enqueue, saving...")
        Cassandra.queueSave(relationType, mutable.Queue(waiter))
      }
    case EnqueueRequest(waiters @ _*) =>
      logger.trace(s"enqueue $waiters")
      if (!capacityOvercome) {
        queue.enqueue(waiters: _*)
        checkOvercome()
      } else {
        logger.debug(s"wouldn't enqueue, saving...")
        Cassandra.queueSave(relationType, waiters)
      }
  }
  
  private def checkOvercome(): Unit = {
    if (!capacityOvercome && queue.size >= queueCapacity) {
      logger.trace(s"queue capacity overcome")
      capacityOvercome = true
    } else if (capacityOvercome && queue.isEmpty) {
      // TODO this may be atomic transaction with last batch delete mutation
      logger.trace(s"queue empty")
      val q = Cassandra.queueLoad(relationType, queueCapacity)
      logger.trace(s"queue loaded: \n$q")
      queue ++= q
      capacityOvercome = false
      Cassandra.queueDelete(relationType, q)
    }
  }
  
  override def postStop(): Unit = {
    logger.info(s"terminating queue keeper")
    Cassandra.queueSave(relationType, queue)
    super.postStop()
  }
  
}

object QueueKeeperActor {
  
  case class EnqueueRequest(waiter: Waiter*)
  
  object DequeueMessage
  
}