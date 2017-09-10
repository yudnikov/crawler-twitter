package ru.yudnikov.crawler.twitter.storage

import java.util.UUID

import com.datastax.driver.core._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import ru.yudnikov.crawler.twitter.TwitterApp.cassandra
import ru.yudnikov.crawler.twitter.enums.Collectibles.Collectibles
import ru.yudnikov.crawler.twitter.{Dependencies, Waiter}
import ru.yudnikov.crawler.twitter.enums.{Collectibles, Markers}
import ru.yudnikov.crawler.twitter.utils.Loggable

import scala.collection.JavaConverters._
import scala.collection.{GenIterable, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** Cassandra API
  *
  * crazy boilerplate of random cassandra commands without any generalization...
  * absolutely unusable for other tasks, but fast implemented
  */
class Cassandra(config: Config, sparkContext: SparkContext) extends Loggable {
  
  private lazy val host: String = config.getString("cassandra.host")
  private lazy val port: Int = config.getInt("cassandra.port")
  private lazy val cluster: Cluster = Cluster.builder().addContactPoint(host).withPort(port).build()

  lazy val keyspace: String = config.getString("cassandra.keyspace")
  
  lazy protected val session: Session = cluster.connect()
  
  /** Terminates the session and closes cluster */
  def terminate(): Unit = {
    session.close()
    cluster.close()
  }
  
  /** Executes query in current session
    *
    * @param query string statement to execute
    * @return Success(ResultSet) or Failure(Exception )
    */
  protected def execute(query: String): Try[ResultSet] = {
    try {
      logger.debug(Markers.STORING, s"executing query: \n$query")
      Success(session.execute(query))
    } catch {
      case e: Exception =>
        logger.error(s"can't execute query: \n$query", e)
        Failure(e)
    }
  }
  
  /** Executes query with parameters "?" in current session
    *
    * @param query string statement to execute
    * @param values sequence of query parameters
    * @return Success(ResultSet) or Failure(Exception )
    */
  protected def execute(query: String, values: Seq[AnyRef]): Try[ResultSet] = {
    try {
      logger.debug(Markers.STORING, s"executing query with values: \n" +
        s"\tquery = $query\n" +
        s"\tvalues = $values")
      Success(session.execute(query, values: _*))
    } catch {
      case e: Exception =>
        logger.error(s"can't execute query: \n$query", e)
        Failure(e)
    }
  }
  
  /** Executes collection of strings statements as batch, has no return
    *
    * @param queries sequence of statements
    */
  def executeBatch(queries: GenIterable[String]): Unit = {
    if (queries.nonEmpty) {
      execute(queries.mkString("BEGIN BATCH\n", ";\n", ";\nAPPLY BATCH;"))
    }
  }
  
  /** Executes collection of strings statements with parameters as batch, has no return
    *
    * @param queries sequence of statements
    * @param values sequence of query parameters
    */
  def executeBatch(queries: GenIterable[String], values: Seq[AnyRef]): Unit = {
    if (queries.nonEmpty) {
      execute(queries.mkString("BEGIN BATCH\n", ";\n", ";\nAPPLY BATCH;"), values)
    }
  }
  
  /** Creates current keyspace if not exists */
  def createKeyspace(): Unit = {
    execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
  }
  
  /** Drops current keyspace if exists */
  def dropKeyspace(): Unit = {
    execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
  }
  
  // Twitter
  
  /** Prepares */
  def prepareStorage(): Unit = {
    createKeyspace()
    waitersQueueCreateTable(Collectibles.FRIENDS)
    waitersQueueCreateTable(Collectibles.FOLLOWERS)
    longsQueueCreateTable(Collectibles.LOOKUP)
    membersCreateTable()
    idsCreateTable(Collectibles.FRIENDS)
    idsCreateTable(Collectibles.FOLLOWERS)
    lookupCreateTable()
    performanceCreateTable()
  }
  
  val createTable = "CREATE TABLE IF NOT EXISTS"
  val _dropTable = "DROP TABLE IF EXISTS"
  
  /** Creates members table */
  def membersCreateTable(): Unit = {
    execute(s"$createTable $keyspace.members (id bigint PRIMARY KEY);")
  }
  
  /** Drops members table */
  def membersDropTable(): Unit = {
    execute(s"${_dropTable} $keyspace.members;")
  }
  
  /** Inserts new id into members table */
  def membersInsert(ids: Long*): Unit = {
    if (ids.nonEmpty) {
      val q = ids.map { id =>
        s"INSERT INTO $keyspace.members (id) values ($id)"
      }
      executeBatch(q)
    }
  }
  
  /** Returns single id's persistence in members table boolean value
    *
    * @param id long value to check
    */
  def membersExists(id: Long): Boolean = {
    execute(s"SELECT id FROM $keyspace.members WHERE id = $id") match {
      case Success(rs) =>
        val rows = rs.iterator().asScala
        if (rows.hasNext)
          true
        else
          false
      case Failure(exception) =>
        throw exception
    }
  }
  
  /** DO NOT USE IT, use membersNonExistingSpark instead
    *
    * @param ids seq of ids to check
    * @return list of longs which are not exist in members table
    */
  @deprecated
  def membersNonExisting(ids: Long*): List[Long] = {
    execute(s"SELECT id FROM $keyspace.members WHERE id IN (${ids.mkString(",")})") match {
      case Success(rs) =>
        val existing = rs.iterator().asScala.map(row => row.get("id", classOf[Long])).toList
        ids.diff(existing).toList
      case Failure(exception) =>
        throw exception
    }
  }
  
  import com.datastax.spark.connector._
  
  private lazy val membersTable = sparkContext.cassandraTable(keyspace, "members")
  
  /** Checks existence of ids in members table using spark cassandra connector, works faster
    *
    * @param ids seq of ids to check
    * @return list of longs which are not exist in members table
    */
  def membersNonExistingSpark(ids: Long*): List[Long] = {
    logger.info(s"started checking ids (${ids.length}) for existence with Spark @ ${new DateTime().toLocalDateTime}")
    val existing = membersTable.filter(row => ids.contains(row.getLong("id"))).map(_.getLong("id")).toLocalIterator.toList
    val result = ids.diff(existing).toList
    logger.info(s"finished @ ${new DateTime().toLocalDateTime}")
    result
  }
  
  lazy val tables: Map[Collectibles, CassandraTableScanRDD[CassandraRow]] = Map(
    Collectibles.MEMBERS -> sparkContext.cassandraTable(keyspace, Collectibles.MEMBERS.toString.toLowerCase()),
    Collectibles.FOLLOWERS -> sparkContext.cassandraTable(keyspace, Collectibles.FOLLOWERS.toString.toLowerCase()),
    Collectibles.FRIENDS -> sparkContext.cassandraTable(keyspace, Collectibles.FRIENDS.toString.toLowerCase()),
    Collectibles.LOOKUP -> sparkContext.cassandraTable(keyspace, Collectibles.LOOKUP.toString.toLowerCase())
  )
  
  /** Returns table's length
    *
    * @param name Collectibles enumeration value
    */
  def tableLength(name: Collectibles): Long = {
    val table = tables(name)
    if (table.isEmpty()) {
      0L
    } else {
      table.map(_ => 1).reduce(_ + _)
    }
  }
  
  /** Creates table for queue of waiters
    *
    * @param name collectible enum value (applicable friends or followers)
    */
  def waitersQueueCreateTable(name: Collectibles): Unit = {
    execute(s"$createTable $keyspace.${name.toString.toLowerCase}_queue (id bigint, cursor bigint, PRIMARY KEY (id, cursor));")
  }
  
  /** Drops table for queue of waiters
    *
    * @param name collectible enum value (applicable friends or followers)
    */
  def waitersQueueDropTable(name: Collectibles): Unit = {
    execute(s" ${_dropTable} $keyspace.${name.toString.toLowerCase}_queue;")
  }
  
  /** Saves queue
    *
    * @param name collectible enum value (applicable friends or followers)
    * @param queue collection of waiters
    */
  def waitersQueueSave(name: Collectibles, queue: GenIterable[Waiter]): Unit = {
    val q = queue.map {
      case Waiter(id: Long, cursor: Long) =>
        s"INSERT INTO $keyspace.${name.toString.toLowerCase}_queue (id, cursor) values ($id, $cursor)"
    }
    executeBatch(q)
  }
  
  /** Loads queue from table
    *
    * @param name collectible enum value (applicable friends or followers)
    * @param size select first n rows
    * @return queue of waiters
    */
  def waitersQueueLoad(name: Collectibles, size: Int = 0): mutable.Queue[Waiter] = {
    val q = s"SELECT id, cursor FROM $keyspace.${name.toString.toLowerCase}_queue${if (size > 0) s" LIMIT $size" else ""};"
    val queue = new mutable.Queue[Waiter]()
    execute(q) match {
      case Success(resultSet) =>
        val seq = resultSet.iterator().asScala.map { row =>
          val id = row.get("id", classOf[Long])
          val cursor = row.get("cursor", classOf[Long])
          Waiter(id, cursor)
        }.toSeq
        seq.foreach(queue.enqueue(_))
        queue
      case _ =>
        queue
    }
  }
  
  /** Deletes following collection of waiters from queue table
    *
    * @param name collectible enum value
    * @param queue collection of waiters
    */
  def waitersQueueDelete(name: Collectibles, queue: GenIterable[Waiter]): Unit = {
    val q = queue.map {
      case Waiter(id: Long, cursor: Long) =>
        s"DELETE FROM $keyspace.${name}_queue WHERE id = $id AND cursor = $cursor"
    }
    executeBatch(q)
  }
  
  /** Creates table for queue with single id column of long type
    *
    * @param name collectible enum value (assume LOOKUPS)
    */
  def longsQueueCreateTable(name: Collectibles): Unit = {
    execute(s"$createTable $keyspace.${name}_queue (id bigint, PRIMARY KEY (id));")
  }
  
  /** Drops table for queue
    *
    * @param name collectible enum value (assume LOOKUPS)
    */
  def longsQueueDropTable(name: Collectibles): Unit = {
    execute(s" ${_dropTable} $keyspace.${name}_queue;")
  }
  
  /** Saves queue
    *
    * @param name collectible enum value (assume LOOKUPS)
    * @param queue collection of long numbers
    */
  def longsQueueSave(name: Collectibles, queue: GenIterable[Long]): Unit = {
    val q = queue.map { id: Long =>
      s"INSERT INTO $keyspace.${name}_queue (id) values ($id)"
    }
    executeBatch(q)
  }
  
  /** Loads queue from table
    *
    * @param name collectible enum value (assume LOOKUPS)
    * @param size select first n rows
    * @return queue of long numbers
    */
  def longsQueueLoad(name: String, size: Int = 0): mutable.Queue[Long] = {
    val q = s"SELECT id FROM $keyspace.${name}_queue${if (size > 0) s" LIMIT $size" else ""};"
    execute(q) match {
      case Success(resultSet) =>
        val queue = new mutable.Queue[Long]()
        val seq = resultSet.iterator().asScala.map { row =>
          row.get("id", classOf[Long])
        }.toSeq
        seq.foreach(queue.enqueue(_))
        queue
    }
  }
  
  /** Deletes following collection of long numbers from queue table
    *
    * @param name collectible enum value (assume LOOKUPS)
    * @param queue collection of long numbers
    */
  def longsQueueDelete(name: String, queue: GenIterable[Long]): Unit = {
    val q = queue.map { id: Long =>
      s"DELETE FROM $keyspace.${name}_queue WHERE id = $id"
    }
    executeBatch(q)
  }
  
  /** Creates table to keep some relation of ids (friend(id1, id2) or follower(id1, id2))
    *
    * @param name collectible enum value (assume FRIENDS of FOLLOWERS)
    */
  def idsCreateTable(name: Collectibles): Unit = {
    execute(s"$createTable $keyspace.$name (id bigint, cursor bigint, $name set<bigint>, PRIMARY KEY (id, cursor));")
  }
  
  /** Drops table by collectible enum value
    *
    * @param name collectible enum value
    */
  def dropTable(name: Collectibles): Unit = {
    dropTable(name.toString)
  }
  
  /** Drops table by table name
    *
    * @param name table name
    */
  def dropTable(name: String): Unit = {
    execute(s" ${_dropTable} $keyspace.$name;")
  }
  
  /** Saves relation between id and list of ids as future
    *
    * Future is used because this section of data is not critical and program would still collect data even
    * if some of this data would be lost
    *
    * @param name collectible enum value
    * @param id source id
    * @param cursor cursor position
    * @param ids list of friends or followers
    * @return returns future unit result of execution
    */
  def idsSave(name: Collectibles, id: Long, cursor: Long, ids: List[Long]): Future[Try[Unit]] = Future {
    Try {
      execute(s"INSERT INTO $keyspace.$name (id, cursor, $name) VALUES ($id, $cursor, {${ids.distinct.mkString(",")}});")
    }
  }
  
  /** Creates table to keep retrieved user data */
  def lookupCreateTable(): Unit = {
    execute(s"$createTable $keyspace.lookup (id bigint PRIMARY KEY, data text);")
  }
  
  /** Saves user data into table
    *
    * @param data Map[Long, String] where key is id and value is json
    */
  def lookupSave(data: Map[Long, String]): Unit = {
    data map { t =>
      execute(s"INSERT INTO $keyspace.lookup (id, data) VALUES (${t._1}, ?)", Seq(t._2))
    }
  }
  
  /** Creates table for performance counting */
  def performanceCreateTable(): Unit = {
    execute(s"$createTable $keyspace.performance (id timestamp PRIMARY KEY, seconds int, data varchar);")
  }
  
  /** Saves performance benchmark result to table with timestamp primary key
    *
    * @param seconds time interval of benchmark
    * @param data json string
    */
  def performanceSave(seconds: Int, data: String): Unit = {
    execute(s"INSERT INTO $keyspace.performance (id, seconds, data) VALUES (${new DateTime().getMillis}, $seconds, ?);", Seq(data))
  }
}
