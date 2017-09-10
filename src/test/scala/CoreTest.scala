import akka.actor.{ActorRef, Props}
import org.scalatest.FlatSpec
import ru.yudnikov.crawler.twitter.Dependencies
import ru.yudnikov.crawler.twitter.actors.DispatcherActor
import ru.yudnikov.crawler.twitter.utils.Loggable

/**
  * Created by Don on 10.09.2017.
  */
class CoreTest extends FlatSpec {
  
  var testDependencies: Dependencies = _
  
  "Dependencies" should "be able to instantiate with application and test configs" in {
    testDependencies = Dependencies(s"test.conf")
    Dependencies(s"application.conf")
  }
  
  "Storage" should "be prepared" in {
    testDependencies.cassandra.dropKeyspace()
    testDependencies.cassandra.prepareStorage()
  }
  
  var dispatcher: ActorRef = _
  
  "Dispatcher" should "be able to be instantiated" in {
    dispatcher = testDependencies.actorSystem.actorOf(Props(classOf[DispatcherActor], testDependencies))
  }
  
  it should "receive a start message" in {
    dispatcher ! DispatcherActor.StartMessage
  }
  
}
