package ru.yudnikov.crawler

import org.joda.time.DateTime
import ru.yudnikov.trash.Loggable

/**
  * Created by Don on 08.09.2017.
  */
object Commons extends Loggable {
  
  def thread(body: => Unit): Thread = {
    val t = new Thread {
      override def run(): Unit = body
    }
    t.start()
    t
  }
  
  def repeat(body: => Unit, n: Int = 100000): Unit = {
    println(benchmark(body))
  }
  
  def benchmark(body: => Unit, startMessage: String = "started benchmark @", finishMessage: String = "finished benchmark @"): String = {
    val start = new DateTime()
    body
    startMessage.replace("@", s"@ ${start.toLocalTime}") + "\n" +
      finishMessage.replace("@", s"@ ${new DateTime().getMillis - start.getMillis} ms")
  }
}
