package ru.yudnikov.trash

import java.io.{File, PrintWriter}
import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import ru.yudnikov.crawler.twitter.utils.TwitterUtils
import ru.yudnikov.crawler.twitter.utils.{Commons, TwitterUtils}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by Don on 06.09.2017.
  */
object TwitterApp extends App with Loggable {
  
  val config: Config = ConfigFactory.parseFile(new File(s"src/main/resources/application.conf"))
  
  val twitter: Twitter = {
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(config.getString("twitter.OAuth.ConsumerKey3"))
      .setOAuthConsumerSecret(config.getString("twitter.OAuth.ConsumerSecret3"))
      .setOAuthAccessToken(config.getString("twitter.OAuth.AccessToken3"))
      .setOAuthAccessTokenSecret(config.getString("twitter.OAuth.AccessTokenSecret3"))
    val tf = new TwitterFactory(cb.build)
    tf.getInstance
  }
  
  val sc: SparkContext = {
    val appName = config.getString("spark.appName")
    val master = config.getString("spark.master")
    val sparkConf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)
    logger.trace("")
    new SparkContext(sparkConf)
  }
  
  val pool = Executors.newScheduledThreadPool(16)
  
  val members: ConcurrentHashMap[Long, Option[Any]] = new ConcurrentHashMap[Long, Option[Any]]()
  
  def addMembers(newMembers: List[Long]): Unit = {
    members.putAll(newMembers.map(id => id -> None).toMap.asJava)
  }
  
  val q = new LinkedBlockingQueue[Long]()
  
  def run(): Unit = {
    val ids0 = List(1L, 2L, 3L)
    ids0.foreach { id =>
      q.put(id)
    }
    addMembers(ids0)
  }
  
  val random = new Random(390)
  
  def process(): Unit = {
    val current = q.poll()
    for (_ <- 1 to 5000) {
      val follower = random.nextInt(1000000000)
      if (!members.containsKey(follower.toLong)) {
        members.put(follower, None)
        q.put(follower)
      }
    }
  }
  
  @tailrec
  def list(length: Int = 1000, i: Long = 0L, l: List[Long] = Nil): List[Long] = {
    if (i >= length) l else list(length, i + 1, i :: l)
  }
  
  def test(): Unit = {
    /*def x = {
      for (i <- 1 to 8) {
        Commons.thread({
          for (j <- 1 to 1000000000) {
            members.put(i * j, None)
          }
        }).join()
      }
      println(members.size())
    }*/
    Commons.repeat({
      println(list(1000000).length)
    }, 1)
    val lists = for {
      i <- 0 to 390 by 10
    } yield (i, i + 10)
    val rdd = sc.parallelize(lists)
    val rdd2 = rdd.map(t => (t._1 * 100000, t._2 * 100000))
    val rdd3 = rdd2.map(t => list(t._2, t._1.toLong)).reduce((a, b) => a ::: b)
    println(rdd3.length)
  }
  
  //test()
  
  //println(members.size())
  
  //run()
  
  def processRunnable = new Runnable {
    override def run(): Unit = process()
  }
  
  def printRunnable = new Runnable {
    override def run(): Unit = {
      val size = q.size()
      val dSize = q.toArray.distinct.length
      logger.info(s"q ($size, $dSize)")
      logger.info(s"members (${members.size()})")
    }
  }
  
  //pool.scheduleAtFixedRate(processRunnable, 0, 1, TimeUnit.MILLISECONDS)
  
  //pool.scheduleAtFixedRate(printRunnable, 0, 3, TimeUnit.SECONDS)
  
  //Thread.sleep(10000)
  
  //null
  
  val screenNames0: List[String] = config.getStringList("twitter.startPages").asScala.toList.map(TwitterUtils.screenNameFromURL)
  logger.trace(s"zero screen names (${screenNames0.length}) are: $screenNames0")
  
  lazy val screenNames: RDD[String] = sc.parallelize(screenNames0)
  
  val ids0: List[Long] = config.getLongList("twitter.startIDs").asScala.toList.asInstanceOf[List[Long]].union(List(twitter.getId))
  logger.trace(s"zero IDs (${ids0.length}) are: $ids0")
  
  lazy val ids: RDD[Long] = sc.parallelize(ids0)
  
  val q1 = new LinkedBlockingQueue[Any]()
  
  screenNames0.foreach { screenName =>
    q1.put(screenName)
  }
  ids0.foreach { id =>
    q1.put(id)
  }
  
  logger.trace(s"zero followers queue (${q1.size()}) is: $q1")
  
  def processFollowers(): Unit = {
    
    def saveFollowers(folder: String, cursor: Long, followers: Array[Long], hasNext: Boolean = false): Unit = {
      logger.trace(s"saving followers to file: " +
        s"\tfolder = $folder,\n" +
        s"\tcursor = $cursor,\n" +
        s"\tfollowers.length = ${followers.length},\n" +
        s"\thasNext = $hasNext")
      val dir = new File(s"data/followers/$folder")
      if (!dir.exists()) {
        logger.trace(s"creating folder ${dir.getAbsolutePath}")
        dir.mkdirs()
      }
      val filename = cursor.toString + (if (hasNext) "@" else "")
      val pw = new PrintWriter(new File(s"${dir.getAbsolutePath}/$filename.csv"))
      pw.write(followers.mkString(";"))
      pw.close()
    }
    
    val current = q1.poll()
    logger.trace(s"getting followers for $current")
    val result = current match {
      case id: Long =>
        val cursor = -1L
        (id, cursor, twitter.getFollowersIDs(id, cursor))
      case (id: Long, cursor: Long) =>
        (id, cursor, twitter.getFollowersIDs(id, cursor))
      case screenName: String =>
        val cursor = -1L
        (screenName, cursor, twitter.getFollowersIDs(screenName, cursor))
      case (screenName: String, cursor: Long) =>
        (screenName, cursor, twitter.getFollowersIDs(screenName, cursor))
      case x: Any =>
        val msg = s"can't handle $x"
        logger.error(msg)
        throw new Exception(msg)
    }
    
    val id = result._1
    val cursor = result._2
    val ids = result._3
    
    if (ids.hasNext) {
      q1.put(current -> ids.getNextCursor)
    }
    
    if (ids.getIDs.length > 0) {
      id match {
        case id: Long =>
          saveFollowers(id.toString, cursor, ids.getIDs, ids.hasNext)
        case screenName: String =>
          saveFollowers(screenName, cursor, ids.getIDs, ids.hasNext)
        case x: Any =>
          val msg = s"can't match $x"
          logger.error(msg)
          throw new Exception(msg)
      }
    }
    
    ids.getIDs.foreach { id =>
      if (!q1.contains(id)) q1.put(id)
    }
  }
  
  pool.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = processFollowers()
  }, 0, 5, TimeUnit.SECONDS)
  
  
}