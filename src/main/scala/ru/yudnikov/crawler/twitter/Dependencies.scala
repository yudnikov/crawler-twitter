package ru.yudnikov.crawler.twitter

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import ru.yudnikov.crawler.twitter.storage.Cassandra
import ru.yudnikov.crawler.twitter.utils.{TwitterUtils, Utils}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterFactory}

import scala.util.Random

/** Common dependencies */
case class Dependencies(configName: String = "application.conf") {
  
  lazy val config: Config = ConfigFactory.parseFile(new File(s"src/main/resources/$configName"))
  
  lazy val actorSystem = ActorSystem(config.getString("appName"))
  
  lazy val sparkContext: SparkContext = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(config.getString("appName"))
      .setMaster(config.getString("spark.master"))
    new SparkContext(sparkConf)
  }
  
  lazy val sc2: SparkContext = {
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", config.getString("cassandra.host"))
      .set("spark.cassandra.auth.username", config.getString("cassandra.username"))
      .set("spark.cassandra.auth.password", config.getString("cassandra.password"))
      .setAppName(config.getString("appName"))
      .setMaster(config.getString("spark.master"))
    new SparkContext(sparkConf)
  }
  
  lazy val random = new Random()
  
  lazy val cassandra = new Cassandra(config, sparkContext)
  
  lazy val twitters: List[Twitter] = Utils.getMapsFromConfig(config, "twitter.OAuths").map { map =>
    TwitterUtils.getTwitter(map.asInstanceOf[Map[String, String]])
  }
  
}
