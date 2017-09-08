package ru.yudnikov.crawler

import akka.actor.{ActorSystem, Props}
import ru.yudnikov.trash.twitter.{Cassandra, Dependencies}
import ru.yudnikov.trash.twitter.Dependencies.config

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.StdIn

/**
  * Created by Don on 07.09.2017.
  */
object MyApp extends App {
  
  val actorSystem = Dependencies.actorSystem
  
  def start(): Unit = {
    
    Cassandra.dropKeyspace()
    Cassandra.createKeyspace()
    Cassandra.waitersQueueCreateTable("friends")
    Cassandra.waitersQueueCreateTable("followers")
    Cassandra.membersCreateTable()
    Cassandra.idsCreateTable("friends")
    Cassandra.idsCreateTable("followers")
    
    val dispatcherActor = actorSystem.actorOf(Props(classOf[DispatcherActor], actorSystem.scheduler))
    
    dispatcherActor ! DispatcherActor.StartMessage
    
  }
  
  println(s"press >>> ENTER <<< to terminate")
  
  start()
  
  StdIn.readLine()
  
  Await.result(actorSystem.terminate(), Duration.Inf)
  Cassandra.terminate()

}
