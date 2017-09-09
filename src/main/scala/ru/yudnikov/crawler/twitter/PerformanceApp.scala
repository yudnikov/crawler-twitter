package ru.yudnikov.crawler.twitter

import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import ru.yudnikov.crawler.twitter.enums.Collectibles
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.crawler.twitter.storage.Cassandra.keyspace

import scala.math.BigDecimal.RoundingMode

/**
  * Created by Don on 09.09.2017.
  */
object PerformanceApp extends App {
  
  import com.datastax.spark.connector._
  
  val performanceTable = Dependencies.sparkContext.cassandraTable(Cassandra.keyspace, "performance")
  
  implicit val formats = DefaultFormats
  
  val flat = performanceTable
    .map(row => row.getInt("seconds") -> Serialization.read[Map[String, Long]](row.getString("data")))
    .flatMap(t => t._2.map(t2 => t2._1 -> (t2._2 -> t._1)))
  
  val average = flat.reduceByKey((t1, t2) => (t1._1 + t2._1) -> (t1._2 + t2._2)).map(t => t._1 -> (BigDecimal(t._2._1) / t._2._2).setScale(2, RoundingMode.HALF_DOWN))
  
  average.foreach(println)
  
}
