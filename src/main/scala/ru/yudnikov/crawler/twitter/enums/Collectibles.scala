package ru.yudnikov.crawler.twitter.enums

/** Fundamental enumeration, means "what to collect" */
object Collectibles extends Enumeration {
  
  type Collectibles = Value
  
  val MEMBERS, FOLLOWERS, FRIENDS, LOOKUP = Value
  
}
