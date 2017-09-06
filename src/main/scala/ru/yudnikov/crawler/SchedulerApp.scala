package ru.yudnikov.crawler

import java.util.concurrent.{Callable, Executors, LinkedBlockingQueue, TimeUnit}

import scala.collection.immutable.Queue



/**
  * Created by Don on 06.09.2017.
  */
object SchedulerApp extends App {
  
  val q = new LinkedBlockingQueue[String]()
  q.put("zhir")
  q.put("naval")
  q.put("xenia")
  
  val pool = Executors.newScheduledThreadPool(4)
  
  def process = new Runnable {
    override def run(): Unit = {
      val current = q.poll()
      q.put(current + "x")
      q.put(current + "y")
      println(s"processing $current")
    }
  }
  
  def print = new Runnable {
    override def run(): Unit = {
      println(s"queue (${q.size()}) is $q")
    }
  }
  
  pool.scheduleAtFixedRate(process, 0, 3, TimeUnit.SECONDS)
  
  pool.scheduleWithFixedDelay(process, 0, 2, TimeUnit.SECONDS)
  
  pool.scheduleWithFixedDelay(print, 10, 10, TimeUnit.SECONDS)
  
  /*for (i <- 1 to 10) {
    println(s"scheduling $i")
    pool.schedule(callable, 5, TimeUnit.SECONDS)
  }*/
  
}
