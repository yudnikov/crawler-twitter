package ru.yudnikov.crawler.twitter.enums

import org.slf4j.{Marker, MarkerFactory}

/**
  * Created by Don on 09.09.2017.
  */
object Markers extends Enumeration {
  
  type Markers = Value
  
  val PERFORMANCE, COLLECTING, DISPATCHING, STORING, QUEUEING = Value
  
  def marker(v: Markers.Value): Marker = MarkerFactory.getMarker(v.toString)
  
  // Some implicit magic :)
  implicit def asMarker(value: Markers.Value): Marker = marker(value)
  
}
