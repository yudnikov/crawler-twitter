package ru.yudnikov.trash.twitter

import java.util.concurrent.{LinkedBlockingDeque, LinkedBlockingQueue}

import com.datastax.driver.core.{Cluster, ResultSet, Session}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import ru.yudnikov.trash.Loggable

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by Don on 06.09.2017.
  */
object _Cassandra extends Loggable {
  
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
  
  protected def execute(query: String): Try[ResultSet] = {
    try {
      logger.info(s"executing query: \n$query")
      Success(session.execute(session.prepare(query).bind()))
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
  
  def membersNonExisting(ids: Long*): List[Long] = {
    execute(s"SELECT id FROM $keyspace.members WHERE id IN (${ids.mkString(",")})") match {
      case Success(rs) =>
        val existing = rs.iterator().asScala.map(row => row.get("id", classOf[Long])).toList
        ids.diff(existing).toList
      case Failure(exception) =>
        throw exception
    }
  }
  
  def membersInsert(ids: Long*): Unit = {
    val q = ids.map { id =>
      s"INSERT INTO $keyspace.members (id) values ($id)"
    }.mkString("BEGIN BATCH\n", ";\n", ";\nAPPLY BATCH;")
    execute(q)
  }
  
  def queueCreateTable(): Unit = {
    execute(s"$createTableIfNotExists $keyspace.queue (id bigint PRIMARY KEY, cursor bigint);")
  }
  
  def queueDropTable(): Unit = {
    execute(s"$dropTableIfExists $keyspace.queue;")
  }
  
  def saveQueue(queue: LinkedBlockingQueue[(Long, Option[Long])]): Unit = {
    execute(s"TRUNCATE TABLE $keyspace.queue;")
    val q = queue.asScala.map {
        case (id: Long, Some(cursor: Long)) =>
          s"INSERT INTO $keyspace.queue (id, cursor) values ($id, $cursor)"
        case (id: Long, None) =>
          s"INSERT INTO $keyspace.queue (id) values ($id)"
    }.mkString("BEGIN BATCH\n", ";\n", ";\nAPPLY BATCH;")
    execute(q)
  }
  
  def loadQueue(): LinkedBlockingQueue[(Long, Option[Long])] = {
    execute(s"SELECT id, cursor FROM $keyspace.queue;") match {
      case Success(resultSet) =>
        val queue = new LinkedBlockingQueue[(Long, Option[Long])]()
        val seq = resultSet.iterator().asScala.map { row =>
          val id = row.get("id", classOf[Long])
          val maybeCursor = row.get("cursor", classOf[Long]) match {
            case cursor: Long if cursor != 0 =>
              Some(cursor)
            case _ =>
              None
          }
          (id, maybeCursor)
        }.toSeq
        seq.foreach(t => queue.put(t._1, t._2))
        queue
    }
  }
  
}
