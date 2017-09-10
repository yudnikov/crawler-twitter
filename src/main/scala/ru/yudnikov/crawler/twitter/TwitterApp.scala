package ru.yudnikov.crawler.twitter

import akka.actor.Props
import ru.yudnikov.crawler.twitter.actors.DispatcherActor
import ru.yudnikov.crawler.twitter.enums.Collectibles
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.crawler.twitter.utils.Loggable

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.StdIn

/** The entry point of application */
object TwitterApp extends App with Loggable {
  
  val dependencies = Dependencies()
  
  val actorSystem = dependencies.actorSystem
  val config = dependencies.config
  
  lazy val cassandra = dependencies.cassandra
  
  private def start(): Unit = {
    
    if (config.getBoolean("dropKeyspace")) {
      logger.warn(s"KEYSPACE WOULD BE DROPPED!")
      cassandra.dropKeyspace()
    }
    
    if (config.getBoolean("prepareStorage")) {
      logger.info(s"keyspace would be prepared")
      cassandra.prepareStorage()
    }
    
    val dispatcherActor = actorSystem.actorOf(Props(classOf[DispatcherActor], dependencies))
    
    dispatcherActor ! DispatcherActor.StartMessage
    
  }
  
  println(s"press >>> ENTER <<< to terminate")
  
  start()
  
  StdIn.readLine()
  
  Await.result(actorSystem.terminate(), Duration.Inf)
  cassandra.terminate()

}
