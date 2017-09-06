package ru.yudnikov.crawler

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
  * Created by Don on 06.09.2017.
  */
trait Loggable {
  
  protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getSimpleName))
  
}
