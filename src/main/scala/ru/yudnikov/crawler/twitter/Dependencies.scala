package ru.yudnikov.crawler.twitter

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.{Twitter, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

import scala.util.Random

/**
  * Created by Don on 06.09.2017.
  */
object Dependencies {
  
  val config: Config = ConfigFactory.parseFile(new File(s"src/main/resources/application.conf"))
  
  val twitter: Twitter = {
    val cb = new ConfigurationBuilder()
      .setDebugEnabled(false)
      .setOAuthConsumerKey(config.getString("twitter.OAuth.ConsumerKey"))
      .setOAuthConsumerSecret(config.getString("twitter.OAuth.ConsumerSecret"))
      .setOAuthAccessToken(config.getString("twitter.OAuth.AccessToken"))
      .setOAuthAccessTokenSecret(config.getString("twitter.OAuth.AccessTokenSecret"))
    new TwitterFactory(cb.build).getInstance
  }
  
  val sparkContext: SparkContext = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(config.getString("spark.appName"))
      .setMaster(config.getString("spark.master"))
    new SparkContext(sparkConf)
  }
  
  val random = new Random()
  
}
