package ru.yudnikov.crawler

import java.util

import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Created by Don on 07.09.2017.
  */
object Utils {
  
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
