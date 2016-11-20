package com.github.diegopacheco.spark.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object SimpleSparkStreamingApp extends App {
  
  val conf = new SparkConf().setMaster("local[2]").setAppName("SimpleSparkStreamingApp")
  val ssc = new StreamingContext(conf, Seconds(1))
  
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))
  
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()
  
  ssc.start()             
  ssc.awaitTermination()
  
}