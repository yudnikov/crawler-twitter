package ru.yudnikov.crawler.twitter

import twitter4j._

/**
  * Created by Don on 09.09.2017.
  */
object TwitterHttpApp extends App {
  
  val listener = new HttpResponseListener {
    override def httpResponseReceived(event: HttpResponseEvent): Unit = println(event)
  }
  
  val t = Dependencies.twitters.head
  val conf = t.getConfiguration
  val http = HttpClientFactory.getInstance(t.getConfiguration.getHttpClientConfiguration)
  http.get(conf.getRestBaseURL + s"account/verify_credentials.json", null, t.getAuthorization, listener)
  
  Thread.sleep(1000)
  
}
