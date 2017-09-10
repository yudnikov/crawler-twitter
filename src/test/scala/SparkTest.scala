import org.scalatest.{FlatSpec, Matchers}
import ru.yudnikov.crawler.twitter.Dependencies
import ru.yudnikov.crawler.twitter.TwitterApp.actorSystem
import ru.yudnikov.crawler.twitter.enums.Collectibles

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by Don on 10.09.2017.
  */
class SparkTest extends FlatSpec with Matchers {
  
  val testDependencies = Dependencies(s"test.conf")
  
  testDependencies.cassandra.dropKeyspace()
  testDependencies.cassandra.prepareStorage()
  
  private val id = testDependencies.random.nextLong()
  private val ids = for {
    _ <- 1 to 1000
  } yield testDependencies.random.nextLong()
  
  testDependencies.cassandra.membersInsert(ids: _*)
  
  testDependencies.cassandra.tableLength(Collectibles.MEMBERS) should be(1000)
  
  testDependencies.cassandra.membersNonExistingSpark(ids: _*) should be(Nil)
  
  Await.result(testDependencies.actorSystem.terminate(), Duration.Inf)
  testDependencies.cassandra.terminate()
  
}
