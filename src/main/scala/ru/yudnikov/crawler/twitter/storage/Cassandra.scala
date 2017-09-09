package ru.yudnikov.crawler.twitter.storage

import com.datastax.driver.core._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import org.joda.time.DateTime
import ru.yudnikov.crawler.twitter.enums.Collectibles.Collectibles
import ru.yudnikov.crawler.twitter.Waiter
import ru.yudnikov.crawler.twitter.enums.{Collectibles, Markers}
import ru.yudnikov.trash.Loggable
import ru.yudnikov.trash.twitter.Dependencies

import scala.collection.JavaConverters._
import scala.collection.{GenIterable, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Created by Don on 06.09.2017.
  */
object Cassandra extends Loggable {
  
  // guava future to scala future
  implicit class ScalableFuture[T](listenableFuture: ListenableFuture[T]) {
    
    def asScala: Future[T] = {
      val promise = Promise[T]()
      val callback = new FutureCallback[T] {
        
        def onFailure(t: Throwable): Unit = promise.failure(t)
        
        def onSuccess(result: T): Unit = promise.success(result)
        
      }
      Futures.addCallback(listenableFuture, callback)
      promise.future
    }
  }
  
  lazy private val host: String = Dependencies.config.getString("cassandra.host")
  lazy private val port: Int = Dependencies.config.getInt("cassandra.port")
  
  lazy val cluster: Cluster = Cluster.builder().addContactPoint(host).withPort(port).build()
  
  lazy protected val keyspace: String = Dependencies.config.getString("cassandra.keyspace")
  lazy protected val session: Session = cluster.connect()
  
  def terminate(): Unit = {
    session.close()
    cluster.close()
  }
  
  protected def execute(query: String): Try[ResultSet] = {
    try {
      logger.info(Markers.STORING, s"executing query: \n$query")
      Success(session.execute(query))
    } catch {
      case e: Exception =>
        logger.error(s"can't execute query: \n$query", e)
        Failure(e)
    }
  }
  
  protected def execute(query: String, values: Seq[AnyRef]): Try[ResultSet] = {
    try {
      logger.info(Markers.STORING, s"executing query with values: \n" +
        s"\tquery = $query\n" +
        s"\tvalues = $values")
      Success(session.execute(query, values: _*))
    } catch {
      case e: Exception =>
        logger.error(s"can't execute query: \n$query", e)
        Failure(e)
    }
  }
  
  def createKeyspace(): Unit = {
    execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
  }
  
  def dropKeyspace(): Unit = {
    execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
  }
  
  // Twitter
  
  val createTableIfNotExists = "CREATE TABLE IF NOT EXISTS"
  val dropTableIfExists = "DROP TABLE IF EXISTS"
  
  def membersCreateTable(): Unit = {
    execute(s"$createTableIfNotExists $keyspace.members (id bigint PRIMARY KEY);")
  }
  
  def membersDropTable(): Unit = {
    execute(s"$dropTableIfExists $keyspace.members;")
  }
  
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
  
  /**
    * DO NOT USE IT, use membersNonExistingSpark instead
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
  
  private lazy val membersTable = Dependencies.sparkContext.cassandraTable(keyspace, "members")
  
  /**
    * Checks existence of ids in members table using spark cassandra connector, works really fast!
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
  
  private lazy val tables: Map[Collectibles, CassandraTableScanRDD[CassandraRow]] = Map(
    Collectibles.MEMBERS -> Dependencies.sparkContext.cassandraTable(keyspace, Collectibles.MEMBERS.toString.toLowerCase()),
    Collectibles.FOLLOWERS -> Dependencies.sparkContext.cassandraTable(keyspace, Collectibles.FOLLOWERS.toString.toLowerCase()),
    Collectibles.FRIENDS -> Dependencies.sparkContext.cassandraTable(keyspace, Collectibles.FRIENDS.toString.toLowerCase()),
    Collectibles.LOOKUP -> Dependencies.sparkContext.cassandraTable(keyspace, Collectibles.LOOKUP.toString.toLowerCase())
  )
  
  def tableCount(name: Collectibles): Long = {
    val table = tables(name)
    if (table.isEmpty()) {
      0L
    } else {
      table.map(_ => 1).reduce(_ + _)
    }
    
  }
  
  def membersInsert(ids: Long*): Unit = {
    if (ids.nonEmpty) {
      val q = ids.map { id =>
        s"INSERT INTO $keyspace.members (id) values ($id)"
      }
      executeBatch(q)
    }
  }
  
  def waitersQueueCreateTable(name: Collectibles): Unit = {
    execute(s"$createTableIfNotExists $keyspace.${name}_queue (id bigint, cursor bigint, PRIMARY KEY (id, cursor));")
  }
  
  def waitersQueueDropTable(name: String): Unit = {
    execute(s"$dropTableIfExists $keyspace.${name}_queue;")
  }
  
  def waitersQueueSave(name: String, queue: GenIterable[Waiter]): Unit = {
    val q = queue.map {
      case Waiter(id: Long, cursor: Long) =>
        s"INSERT INTO $keyspace.${name}_queue (id, cursor) values ($id, $cursor)"
    }
    executeBatch(q)
  }
  
  def waitersQueueLoad(name: String, size: Int = 0): mutable.Queue[Waiter] = {
    val q = s"SELECT id, cursor FROM $keyspace.${name}_queue${if (size > 0) s" LIMIT $size" else ""};"
    execute(q) match {
      case Success(resultSet) =>
        val queue = new mutable.Queue[Waiter]()
        val seq = resultSet.iterator().asScala.map { row =>
          val id = row.get("id", classOf[Long])
          val cursor = row.get("cursor", classOf[Long])
          Waiter(id, cursor)
        }.toSeq
        seq.foreach(queue.enqueue(_))
        queue
    }
  }
  
  def waitersQueueDelete(name: String, queue: GenIterable[Waiter]): Unit = {
    val q = queue.map {
      case Waiter(id: Long, cursor: Long) =>
        s"DELETE FROM $keyspace.${name}_queue WHERE id = $id AND cursor = $cursor"
    }
    executeBatch(q)
  }
  
  // TODO need some generalization...
  def longsQueueCreateTable(name: Collectibles): Unit = {
    execute(s"$createTableIfNotExists $keyspace.${name}_queue (id bigint, PRIMARY KEY (id));")
  }
  
  def longsQueueDropTable(name: String): Unit = {
    execute(s"$dropTableIfExists $keyspace.${name}_queue;")
  }
  
  def longsQueueSave(name: String, queue: GenIterable[Long]): Unit = {
    val q = queue.map { id: Long =>
      s"INSERT INTO $keyspace.${name}_queue (id) values ($id)"
    }
    executeBatch(q)
  }
  
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
  
  def longsQueueDelete(name: String, queue: GenIterable[Long]): Unit = {
    val q = queue.map { id: Long =>
      s"DELETE FROM $keyspace.${name}_queue WHERE id = $id"
    }
    executeBatch(q)
  }
  
  
  def idsCreateTable(name: Collectibles): Unit = {
    execute(s"$createTableIfNotExists $keyspace.$name (id bigint, cursor bigint, $name set<bigint>, PRIMARY KEY (id, cursor));")
  }
  
  def idsDropTable(name: Collectibles): Unit = {
    execute(s"$dropTableIfExists $keyspace.$name;")
  }
  
  def idsSave(name: Collectibles, id: Long, cursor: Long, ids: List[Long]): Future[Try[Unit]] = Future {
    Try {
      execute(s"INSERT INTO $keyspace.$name (id, cursor, $name) VALUES ($id, $cursor, {${ids.distinct.mkString(",")}});")
    }
  }
  
  
  def lookupCreateTable(): Unit = {
    execute(s"$createTableIfNotExists $keyspace.lookup (id bigint PRIMARY KEY, data text);")
  }
  
  def lookupDropTable(): Unit = {
    execute(s"$dropTableIfExists $keyspace.lookup;")
  }
  
  def lookupSave(data: Map[Long, String]): Unit = {
    val q = data map { t =>
      execute(s"INSERT INTO $keyspace.lookup (id, data) VALUES (${t._1}, ?)", Seq(t._2))
    }
    //executeBatch(q, data.values.toSeq)
  }
  
  def executeBatch(queries: GenIterable[String]): Unit = {
    if (queries.nonEmpty) {
      execute(queries.mkString("BEGIN BATCH\n", ";\n", ";\nAPPLY BATCH;"))
    }
  }
  
  def executeBatch(queries: GenIterable[String], values: Seq[AnyRef]): Unit = {
    if (queries.nonEmpty) {
      execute(queries.mkString("BEGIN BATCH\n", ";\n", ";\nAPPLY BATCH;"), values)
    }
  }
}
