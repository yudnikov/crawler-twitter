package ru.yudnikov.crawler.twitter

import akka.actor.Props
import ru.yudnikov.crawler.twitter.actors.DispatcherActor
import ru.yudnikov.crawler.twitter.enums.Collectibles
import ru.yudnikov.crawler.twitter.storage.Cassandra

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.StdIn

/**
  * Created by Don on 07.09.2017.
  */
object TwitterApp extends App {
  
  val actorSystem = Dependencies.actorSystem
  val config = Dependencies.config
  
  def start(): Unit = {
    
    if (config.getBoolean("prepareStorage")) {
      //Cassandra.dropKeyspace()
      Cassandra.createKeyspace()
      Cassandra.waitersQueueCreateTable(Collectibles.FRIENDS)
      Cassandra.waitersQueueCreateTable(Collectibles.FOLLOWERS)
      Cassandra.longsQueueCreateTable(Collectibles.LOOKUP)
      Cassandra.membersCreateTable()
      Cassandra.idsCreateTable(Collectibles.FRIENDS)
      Cassandra.idsCreateTable(Collectibles.FOLLOWERS)
      Cassandra.lookupCreateTable()
      Cassandra.performanceCreateTable()
    }
    
    val dispatcherActor = actorSystem.actorOf(Props(classOf[DispatcherActor]))
    
    dispatcherActor ! DispatcherActor.StartMessage
    
  }
  
  println(s"press >>> ENTER <<< to terminate")
  
  start()
  
  StdIn.readLine()
  
  Await.result(actorSystem.terminate(), Duration.Inf)
  Cassandra.terminate()

}
