package ru.yudnikov.crawler

import ru.yudnikov.trash.twitter.Dependencies

import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
import com.datastax.spark
import com.datastax.spark._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnector._

/**
  * Created by Don on 07.09.2017.
  */
object TestSpark extends App {
  
  val ks = Dependencies.config.getString("cassandra.keyspace")
  
  val table = Dependencies.sparkContext.cassandraTable(ks, "members").cache()
  
  val longs = table.map(r => r.getLong("id")).filter(_ >= 3314139866L)
  
  longs.foreach(println)
  
}
