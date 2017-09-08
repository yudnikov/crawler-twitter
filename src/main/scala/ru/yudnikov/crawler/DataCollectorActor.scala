package ru.yudnikov.crawler

import akka.actor.{Actor, ActorRef}
import ru.yudnikov.crawler.DataCollectorActor.CollectDataRequest
import ru.yudnikov.crawler.queues.QueueActor
import ru.yudnikov.trash.Loggable
import twitter4j.Twitter

/**
  * Created by Don on 08.09.2017.
  */
class DataCollectorActor(twitter: Twitter, queue: ActorRef) extends Actor with Loggable {
  override def receive: Receive = {
    case CollectDataRequest =>
//      queue ?
//      twitter.users().lookupUsers()
  }
}

object DataCollectorActor {
  
  object CollectDataRequest
  
  case class CollectDataResponse()
  
}
