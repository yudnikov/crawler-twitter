package ru.yudnikov.trash.twitter


import java.util.concurrent.LinkedBlockingQueue

import ru.yudnikov.trash.Loggable

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * Created by Don on 06.09.2017.
  */
object Collector extends App with Loggable {
  
  val queue: LinkedBlockingQueue[(Long, Option[Long])] = new LinkedBlockingQueue[(Long, Option[Long])]()
  
  def testFill(): Unit = {
    _Cassandra.dropKeyspace()
    _Cassandra.createKeyspace()
    _Cassandra.membersCreateTable()
    val f = Future.sequence {
      for (_ <- 1 to 1000) yield Future {
        val ids = for (_ <- 1 to 10000) yield {
          Dependencies.random.nextInt(100000).toLong
      
        }
        _Cassandra.membersInsert(ids: _*)
      }
    }
    Await.result(f, Duration.Inf)
  }
  
  def testContains(): Unit = {
    _Cassandra.dropKeyspace()
    _Cassandra.createKeyspace()
    _Cassandra.membersCreateTable()
    _Cassandra.membersInsert()
    val ids1 = List[Long](88690, 16290, 39618, 57167, 24151)
    val ids2 = ids1 ::: List[Long](57123, 12342)
    _Cassandra.membersInsert(ids1: _*)
    val ne = _Cassandra.membersNonExisting(ids2: _*)
    logger.info(s"non existing are: $ne")
  }
  
  def testQueue(): Unit = {
    _Cassandra.queueCreateTable()
    queue.put(Dependencies.random.nextInt(100000).toLong, None)
    queue.put(Dependencies.random.nextInt(100000).toLong, None)
    queue.put(Dependencies.random.nextInt(100000).toLong, None)
    queue.put(Dependencies.random.nextInt(100000).toLong, Some(Dependencies.random.nextInt(100000).toLong))
    queue.put(Dependencies.random.nextInt(100000).toLong, Some(Dependencies.random.nextInt(100000).toLong))
    queue.put(Dependencies.random.nextInt(100000).toLong, Some(Dependencies.random.nextInt(100000).toLong))
    _Cassandra.saveQueue(queue)
    val q = _Cassandra.loadQueue()
    logger.info(s"loaded queue $q")
  }
  
  /*testFill()
  
  testContains()*/
  
  testQueue()
  
  def collect(): Unit = {
  
  }
  
}
