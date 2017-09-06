package ru.yudnikov.crawler

import java.io.{File, FilenameFilter, PrintWriter}

import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import ru.yudnikov.crawler.MyApp.twitter
import twitter4j.{Twitter, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

import scala.collection.JavaConverters._

/**
  * Created by Don on 05.09.2017.
  */
object MyApp extends App with Loggable {
  
  val conf: Config = ConfigFactory.parseFile(new File(s"src/main/resources/application.conf"))
  
  val cb = new ConfigurationBuilder
  cb.setDebugEnabled(true)
    .setOAuthConsumerKey(conf.getString("twitter.OAuth.ConsumerKey"))
    .setOAuthConsumerSecret(conf.getString("twitter.OAuth.ConsumerSecret"))
    .setOAuthAccessToken(conf.getString("twitter.OAuth.AccessToken"))
    .setOAuthAccessTokenSecret(conf.getString("twitter.OAuth.AccessTokenSecret"))
  val tf = new TwitterFactory(cb.build)
  
  implicit val twitter = tf.getInstance
  
  def getFollowersIDs(screenName: String, cursor: Long = -1)(implicit twitter: Twitter): List[Long] = {
    logger.trace(s"getting follower's IDs for $screenName from cursor $cursor")
    val res = twitter.getFollowersIDs(screenName, -1).getIDs.toList
    logger.trace(s"got $res")
    res
  }
  
  val startPages = conf.getStringList("twitter.startPages").asScala.toList.grouped(15).toList
  
  //println(startPages)
  
  def getScreenName(string: String): String = {
    logger.trace(s"getting screen name from $string")
    val split = string.split("/")
    val res = split(split.length - 1)
    logger.trace(s"got $res")
    res
  }
  
  /*def saveFollowers(screenName: String, cursor: Int = -1): Unit = {
    val files = new File(s"data/followers/$screenName/").listFiles().toList
  }*/
  
  def saveIDs(dir: String, filename: String, ids: List[Long]): Unit = {
    val file = new File(dir)
    if (!file.exists()) {
      logger.trace(s"creating non-exist directory $dir")
      file.mkdir()
    }
    val pw = new PrintWriter(new File(s"$dir/$filename.csv"))
    pw.write(ids.mkString(";"))
    pw.close()
  }
  
  // assume mask is $screenName-$cursor-$length
  def ids0 = startPages.map { list =>
    println(list)
    list.map { url =>
      val screenName = getScreenName(url)
      val followersIDs = getFollowersIDs(screenName)
      println(s"$screenName has ${followersIDs.length} followers")
      
      screenName -> followersIDs
    } toMap
  }
  
  //println(ids0)
  
  def test(): Unit = {
    val url = startPages.flatten.head
    val screenName = getScreenName(url)
    for (cursor <- -1L to 3L) {
      val followersIDs = getFollowersIDs(screenName, cursor)
      saveIDs(s"data/followers/$screenName", s"${cursor}_${followersIDs.length}", followersIDs)
    }
  }
  
  test()
  
}
