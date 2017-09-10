package ru.yudnikov.crawler.twitter.enums

import java.util.UUID

import org.json4s.{CustomKeySerializer, CustomSerializer}
import org.json4s.JsonAST.{JNull, JString}
import org.slf4j.{Marker, MarkerFactory}

/** Logger markers with some implicit magic */
object Markers extends Enumeration {
  
  type Markers = Value
  
  val PERFORMANCE, COLLECTING, DISPATCHING, STORING, QUEUEING, CONFIDENT = Value
  
  def marker(v: Markers.Value): Marker = MarkerFactory.getMarker(v.toString)
  
  /** Implicit conversion to marker
    *
    * @param value enum value
    * @return marker
    */
  implicit def asMarker(value: Markers.Value): Marker = marker(value)
  
  /** JSON serializer */
  case object Serializer extends CustomSerializer[Markers.Value](_ =>
    ( {
      case JString(s) =>
        Markers.withName(s)
      case JNull =>
        null
    }, {
      case value: Markers.Value =>
        JString(value.toString)
      case value: Markers.Val =>
        JString(value.toString)
    })
  )
  
}
