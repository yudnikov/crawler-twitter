package ru.yudnikov.crawler.twitter

import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import ru.yudnikov.crawler.twitter.enums.{Collectibles, Markers}

import ru.yudnikov.crawler.twitter.utils.Loggable

import scala.math.BigDecimal.RoundingMode

/** Performance counting application */
object PerformanceApp extends App with Loggable {
  
  import com.datastax.spark.connector._
  
  val dependencies = Dependencies()
  val performanceTable = dependencies.sparkContext.cassandraTable(dependencies.cassandra.keyspace, "performance")
  
  implicit val formats = DefaultFormats
  
  val parsed = performanceTable
    .map(row => row.getInt("seconds") -> Serialization.read[Map[String, Long]](row.getString("data")))
    .flatMap(t => t._2.map(t2 => t2._1 -> (t2._2 -> t._1)))
  
  val average = parsed
    .reduceByKey((t1, t2) => (t1._1 + t2._1) -> (t1._2 + t2._2))
    .map(t => t._1 -> (BigDecimal(t._2._1) / t._2._2).setScale(2, RoundingMode.HALF_DOWN))
    .toLocalIterator.mkString("\n")

  val msg = s"average performance is :\n$average"
  
  logger.debug(Markers.PERFORMANCE, msg)
  
  println(msg)
  
}
