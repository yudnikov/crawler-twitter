package ru.yudnikov.crawler.twitter

/**
  * Created by Don on 07.09.2017.
  */
case class Waiter(id: Long, cursor: Long = -1L) {
  
}

object Waiter {
  
  implicit def toLong(waiter: Waiter): Long = waiter.id
  
}
