name := "crawler-twitter"

version := "1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.json4s/json4s-native_2.11
libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.3"
// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-core
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.6"
// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
// https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging_2.11
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.5.0"
// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
// https://mvnrepository.com/artifact/commons-logging/commons-logging
libraryDependencies += "commons-logging" % "commons-logging" % "1.2"

// https://mvnrepository.com/artifact/com.typesafe.akka/akka-actor_2.11
libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.17"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += ("org.apache.spark" % "spark-core_2.11" % "2.2.0").exclude("org.slf4j", "*")
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += ("org.apache.spark" % "spark-sql_2.11" % "2.2.0").exclude("org.slf4j", "*")

// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.11
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.5"