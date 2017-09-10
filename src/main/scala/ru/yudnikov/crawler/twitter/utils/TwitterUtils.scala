package ru.yudnikov.crawler.twitter.utils

import ru.yudnikov.crawler.twitter.enums.Markers
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterFactory}

/** Some twitter utilities */
object TwitterUtils extends Loggable {
  
  /** Extracts screen name from url string
    *
    * @param string
    * @return
    */
  def screenNameFromURL(string: String): String = {
    logger.trace(s"getting screen name from $string")
    val split = string.split("/")
    val res = split(split.length - 1)
    logger.trace(s"got $res")
    res
  }
  
  /** Gets twitter from OAuth information map
    *
    * @param map OAuth information
    *            ConsumerKey -> "***"
    *            ConsumerSecret -> "***"
    *            AccessToken -> "***"
    *            AccessTokenSecret -> "***"
    * @param check boolean if true asks to perform basic account check and log the result
    * @return twitter api client instance
    */
  def getTwitter(map: Map[String, String], check: Boolean = true): Twitter = {
    val cb = new ConfigurationBuilder()
      .setDebugEnabled(true)
      .setOAuthConsumerKey(map("ConsumerKey"))
      .setOAuthConsumerSecret(map("ConsumerSecret"))
      .setOAuthAccessToken(map("AccessToken"))
      .setOAuthAccessTokenSecret(map("AccessTokenSecret"))
      .setJSONStoreEnabled(true)
    val twitter = new TwitterFactory(cb.build).getInstance
    if (check) try {
      twitter.getId
      logger.debug(Markers.CONFIDENT, s"got twitter by \n\t$map")
    } catch {
      case e: Exception =>
        logger.error(Markers.CONFIDENT, s"can't get twitter by \n\t$map", e)
    }
    twitter
  }
  
}
