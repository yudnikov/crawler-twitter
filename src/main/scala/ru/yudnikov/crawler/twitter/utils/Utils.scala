package ru.yudnikov.crawler.twitter.utils

import java.util

import com.typesafe.config.Config

import scala.collection.JavaConverters._

/** Some common utilities */
object Utils {
  
  /** Gets list of maps [string, any] from scala config
    *
    * @param config config instance
    * @param path string path to extract
    * @return list of maps
    */
  def getMapsFromConfig(config: Config, path: String): List[Map[String, Any]] = {
    val list = config.getList(path).asScala
    list.flatMap { x =>
      val maps = x.unwrapped().asInstanceOf[util.ArrayList[util.HashMap[String, Any]]].asScala
      maps map { map =>
        map.asScala.toMap
      }
    }.toList
  }
}
