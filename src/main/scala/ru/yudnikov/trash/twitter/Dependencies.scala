package ru.yudnikov.trash.twitter

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.{Twitter, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

import scala.util.Random

/**
  * Created by Don on 06.09.2017.
  */
object Dependencies {
  
  lazy val config: Config = ConfigFactory.parseFile(new File(s"src/main/resources/application.conf"))
  
  lazy val actorSystem = ActorSystem(config.getString("appName"))
  
  lazy val twitter: Twitter = {
    val cb = new ConfigurationBuilder()
      .setDebugEnabled(true)
      .setOAuthConsumerKey(config.getString("twitter.OAuth.ConsumerKey"))
      .setOAuthConsumerSecret(config.getString("twitter.OAuth.ConsumerSecret"))
      .setOAuthAccessToken(config.getString("twitter.OAuth.AccessToken"))
      .setOAuthAccessTokenSecret(config.getString("twitter.OAuth.AccessTokenSecret"))
    new TwitterFactory(cb.build).getInstance
  }
  
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
  
}
