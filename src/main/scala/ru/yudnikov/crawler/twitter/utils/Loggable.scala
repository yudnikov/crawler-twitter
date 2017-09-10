package ru.yudnikov.crawler.twitter.utils

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Loggable trait brings logger in da house */
trait Loggable {
  
  protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getSimpleName))
  
}
