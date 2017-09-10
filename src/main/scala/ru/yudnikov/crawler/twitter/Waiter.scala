package ru.yudnikov.crawler.twitter

/** Basic class, contained in IDs collecting queue
  *
  * @constructor create a new waiter
  * @param id twitter user id
  * @param cursor cursor position from what to take IDs
  */
case class Waiter(id: Long, cursor: Long = -1L)

object Waiter {
  
  implicit def toLong(waiter: Waiter): Long = waiter.id
  
}
